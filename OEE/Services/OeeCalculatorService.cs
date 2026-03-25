using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using OEE.Models; // 引入数据模型

namespace OEE.Services
{
    public class OeeCalculatorService
    {
        // 从 App.config 中读取连接字符串
        // readonly 关键字确保这些关键配置在对象初始化后无法被意外修改，保证底层数据通信通道的安全。
        private readonly string _mainDbConn;
        private readonly string _collectDbConn;

        public OeeCalculatorService()
        {
            _mainDbConn = ConfigurationManager.ConnectionStrings["MainDB"].ConnectionString;
            _collectDbConn = ConfigurationManager.ConnectionStrings["CollectDB"].ConnectionString;

            if (string.IsNullOrEmpty(_mainDbConn) || string.IsNullOrEmpty(_collectDbConn))
            {
                throw new Exception("启动失败：数据库连接配置缺失，请检查 App.config！");
            }
        }

        /// <summary>
        /// OEE 核心计算与调度方法
        /// </summary>
        /// <param name="sysNo">系统编号参数 (对应原存储过程的 @strSysNo)</param>
        /// <param name="targetDate">目标计算日期参数 (对应 @dtDateTime)</param>
        /// <param name="specificLineNo">特定产线编号参数，若为空则计算全部 (对应 @strProductLineNo)</param>
        public void CalculateAndSaveOee(string sysNo, DateTime targetDate, string specificLineNo)
        {
            // 1. 获取主库的基础产线与班次时间信息
            List<ProductLineOee> productLines = GetBaseProductLines(targetDate, specificLineNo);
			var lineScanMap = new Dictionary<ProductLineOee, List<ScanRecord>>();
            foreach (var line in productLines)
		    {
		        line.SysNo = sysNo;
		        
		        var scanRecords = GetScanRecords(line);
		        // 【新增】：将拉取到的扫码记录存入字典
		        lineScanMap[line] = scanRecords; 
		
		        if (scanRecords.Any())
		        {
		            line.FirstOfflineTime = scanRecords.Min(s => s.OccurDate);
		            line.LastOfflineTime = scanRecords.Max(s => s.OccurDate);
		            
		            var lastScan = scanRecords.OrderByDescending(s => s.OccurDate).FirstOrDefault();
		            if (lastScan != null)
		            {
		                line.ProductNo = lastScan.ProductNo;
		            }
		            line.ProducedQuantity = scanRecords.Sum(s => s.DeliveryNum);
		        }
		        else
		        {
		            line.FirstOfflineTime = new DateTime(1900, 1, 1);
		            line.LastOfflineTime = new DateTime(1900, 1, 1);
		        }
		
		        line.RestTime = GetRestTime(line);
		        line.PlanStopTime = GetPlanStopTime(line);
		        
		        double jgPlanStop; 
		        line.AbnormalStopTime = GetAbnormalStopTime(line, out jgPlanStop);
		        line.JgPlanStopTime = jgPlanStop; 
		
		        line.CtValue = GetTheoreticalCt(line);
		        line.ScrapNum = GetScrapQuantity(line);
		
		        ComputeOeeMath(line);
		    }

            // 5. 将计算结果持久化（保存）到主数据库的业务表中
            SaveOeeResultsToDatabase(productLines, targetDate, lineScanMap);
        }

        /// <summary>
        /// 纯数学计算逻辑层：解耦数据库，仅依赖内存对象
        /// </summary>
        private void ComputeOeeMath(ProductLineOee line)
        {
            // 出勤时间 (TotalTime) = 最后一次下线时间 - 班次开始时间 (转化为小时)
            if (line.LastOfflineTime > line.ShiftStartTime)
            {
                line.TotalTime = Math.Round((line.LastOfflineTime - line.ShiftStartTime).TotalSeconds / 3600.0, 2);
            }

            // 扣减《计划停机时间设置》里的固定停机时间
            line.TotalTime -= line.PlanStopTime;

            // 可用时间 = 出勤时间 - 休息时间
            line.CanUseTime = line.TotalTime - line.RestTime;

            // 实际运行 = 可用时间 - 计划停机(间隔) - 异常停机
            line.RunTime = line.CanUseTime - line.JgPlanStopTime - line.AbnormalStopTime;

            // --- OEE 三要素核心算法 ---

            // 1. 时间稼动率 (AvailabilityRate)
            line.AvailabilityRate = line.CanUseTime != 0 ? Math.Round(line.RunTime / line.CanUseTime, 2) : 0;

            // 2. 实际节拍 (RealCtValue) = 实际运行时间(转为秒) / 产量
            line.RealCtValue = line.ProducedQuantity != 0 ? Math.Round((line.RunTime * 3600.0) / line.ProducedQuantity, 0) : 0;
            
            // 性能稼动率 (PerformanceRate) = 理论节拍 / 实际节拍
            line.PerformanceRate = line.RealCtValue != 0 ? Math.Round(line.CtValue / line.RealCtValue, 2) : 0;

            // 3. 合格数与良品率 (QualityRate)
            line.DirectPassParts = line.ProducedQuantity - line.ScrapNum;
            if (line.DirectPassParts < 0) line.DirectPassParts = 0; // 容错处理

            if (line.ProducedQuantity == 0)
            {
                line.QualityRate = 0;
            }
            else
            {
                double tempQuality = line.DirectPassParts / line.ProducedQuantity;
                line.QualityRate = tempQuality > 1 ? 1 : Math.Round(tempQuality, 4);
            }

            // 最终 OEE 综合折算
            line.OeeRate = Math.Round(line.AvailabilityRate * line.PerformanceRate * line.QualityRate * 100, 2);
        }

        // =====================================================================
        // 以下为 ADO.NET 数据访问层 (DAL) 的具体实现
        // =====================================================================

        private List<ProductLineOee> GetBaseProductLines(DateTime date, string lineNo)
        {
            var lines = new List<ProductLineOee>();
            using (SqlConnection conn = new SqlConnection(_mainDbConn))
            {
                string sql = @"
                    SELECT plc.product_line_no, plc.product_line_type_no, ss.start_time, ss.end_time, ss.tomorrow_tag
                    FROM TA05_product_line_code plc
                    LEFT JOIN TA05_product_line_shift_start ss ON ss.product_line_no = plc.product_line_no
                    WHERE (@LineNo = '' OR plc.product_line_no = @LineNo)";
                
                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@LineNo", lineNo ?? "");
                    conn.Open();
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var line = new ProductLineOee
                            {
                                ProductLineNo = reader["product_line_no"].ToString(),
                                ProductLineTypeNo = reader["product_line_type_no"].ToString(),
                                ShiftStartDate = date.Date
                            };

                            // 时间字符串处理：将配置的 '08:00' 等字符串转换为实际的 DateTime 对象
                            string startStr = reader["start_time"] == DBNull.Value ? "08:00" : reader["start_time"].ToString();
                            string endStr = reader["end_time"] == DBNull.Value ? "08:00" : reader["end_time"].ToString();
                            string tomorrowTag = reader["tomorrow_tag"].ToString();

                            line.ShiftStartTime = date.Date.Add(TimeSpan.Parse(startStr));
                            line.ShiftEndTime = date.Date.Add(TimeSpan.Parse(endStr));

                            // 处理跨天班次：如果结束时间带有明日标记，则在日期上加 1 天
                            if (tomorrowTag == "T")
                            {
                                line.ShiftEndTime = line.ShiftEndTime.AddDays(1);
                            }

                            // 历史补偿逻辑：如果开始时间大于传入的时间参数，整体往前推一天
                            if (line.ShiftStartTime > date)
                            {
                                line.ShiftStartTime = line.ShiftStartTime.AddDays(-1);
                                line.ShiftEndTime = line.ShiftEndTime.AddDays(-1);
                                line.ShiftStartDate = line.ShiftStartTime.Date;
                            }

                            lines.Add(line);
                        }
                    }
                }
            }
            return lines;
        }

        private List<ScanRecord> GetScanRecords(ProductLineOee line)
		{
		    var records = new List<ScanRecord>();
		    // 注意：扫码数据通常在数据采集库中，请确保使用正确的数据库连接串
		    using (SqlConnection conn = new SqlConnection(_collectDbConn)) 
		    {
		        string sql = "";
		
		        // 根据产线类型区分查询逻辑
		        if (line.ProductLineTypeNo == "ZP")
		        {
		            // 装配线 (ZP) 逻辑保持不变：使用 OUTER APPLY 逆向推导产品品名
		            sql = @"
		                SELECT 
		                    off.occur_date,
		                    off.delivery_num,
		                    ISNULL(lbl.product_no, '') AS product_no
		                FROM tx02_assm_offline_record off
		                
		                -- 时间序列的就近匹配
		                OUTER APPLY (
		                    SELECT TOP 1 p.product_no 
		                    FROM T200_customer_label_print p 
		                    WHERE p.product_line_no = off.product_line_no 
		                      AND p.print_date <= off.occur_date
		                    ORDER BY p.print_date DESC
		                ) lbl
		                
		                WHERE off.product_line_no = @LineNo
		                  AND off.occur_date >= @StartTime
		                  AND off.occur_date < @EndTime";
		        }
		        else 
		        {
		            // 修复点：非装配线统一使用最后一道工序的扫码视图
		            sql = @"
		                SELECT 
		                    edit_time AS occur_date,
		                    1.0 AS delivery_num, 
		                    ISNULL(product_no, '') AS product_no
		                FROM Vx02_scan_record_last_op
		                WHERE product_line_no = @LineNo
		                  AND edit_time >= @StartTime
		                  AND edit_time < @EndTime";
		        }
		
		        using (SqlCommand cmd = new SqlCommand(sql, conn))
		        {
		            // 参数绑定，防止 SQL 注入
		            cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
		            cmd.Parameters.AddWithValue("@StartTime", line.ShiftStartTime);
		            cmd.Parameters.AddWithValue("@EndTime", line.ShiftEndTime);
		
		            conn.Open();
		            using (SqlDataReader reader = cmd.ExecuteReader())
		            {
		                while (reader.Read())
		                {
		                    records.Add(new ScanRecord
		                    {
		                        OccurDate = Convert.ToDateTime(reader["occur_date"]),
		                        // 使用 ToDouble 兼容视图中常量 1.0 的浮点数转化
		                        DeliveryNum = Convert.ToDouble(reader["delivery_num"]), 
		                        ProductNo = reader["product_no"].ToString()
		                    });
		                }
		            }
		        }
		    }
		    return records;
		}
        private double GetRestTime(ProductLineOee line)
        {
            double rest = 0;
            using (SqlConnection conn = new SqlConnection(_mainDbConn))
            {
                // 提取原存储过程中对于休息时间的计算部分
                string sql = @"
                    SELECT SUM(round(DATEDIFF(second, start_time, 
                        CASE WHEN end_time > @LastOffline THEN @LastOffline ELSE end_time END) / 3600.0, 2)) as rest_time
                    FROM TA05_product_line_shift_rest
                    WHERE product_line_no = @LineNo 
                    AND start_time < @LastOffline";

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
                    cmd.Parameters.AddWithValue("@LastOffline", line.LastOfflineTime);
                    conn.Open();
                    object result = cmd.ExecuteScalar();
                    if (result != DBNull.Value && result != null)
                    {
                        rest = Convert.ToDouble(result);
                    }
                }
            }
            return rest < 0 ? 0 : rest; // 防御性编程：防止负数时间污染计算结果
        }

        private double GetPlanStopTime(ProductLineOee line)
        {
            double planStop = 0;
            using (SqlConnection conn = new SqlConnection(_mainDbConn))
            {
                string sql = @"
                    SELECT SUM(round(DATEDIFF(second, start_time, 
                        CASE WHEN end_time > @LastOffline THEN @LastOffline ELSE end_time END) / 3600.0, 2)) as stop_time
                    FROM TA05_plan_stop_time_set
                    WHERE product_line_no = @LineNo 
                    AND start_time >= @Start AND start_time < @End AND start_time < @LastOffline";

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
                    cmd.Parameters.AddWithValue("@Start", line.ShiftStartTime);
                    cmd.Parameters.AddWithValue("@End", line.ShiftEndTime);
                    cmd.Parameters.AddWithValue("@LastOffline", line.LastOfflineTime);
                    conn.Open();
                    object result = cmd.ExecuteScalar();
                    if (result != DBNull.Value && result != null)
                    {
                        planStop = Convert.ToDouble(result);
                    }
                }
            }
            return planStop < 0 ? 0 : planStop;
        }

       private double GetAbnormalStopTime(ProductLineOee line, out double jgPlanStopTime)
		{
		    double abnormalStop = 0;
		    jgPlanStopTime = 0;
		
		    using (SqlConnection conn = new SqlConnection(_mainDbConn))
		    {
		        // 核心修改：在 WHERE 子句中补齐了 start_time >= @Start 的时间轴边界约束
		        string sql = @"
		            SELECT 
		                SUM(plhr.diff_time) as total_diff, 
		                ISNULL(har.halt_type, ISNULL(harjjg.halt_type, '异常停机')) as final_halt_type
		            FROM T200_product_line_hour_diff_result plhr
		            
		            -- 1. 尝试获取装配线 (ZP) 的最新异常报告
		            OUTER APPLY (
		                SELECT TOP 1 halt_type 
		                FROM TB01_zp_hour_analyse_report 
		                WHERE product_line_no = plhr.product_line_no 
		                  AND start_time = plhr.start_time 
		                ORDER BY qpr_sn DESC
		            ) har
		            
		            -- 2. 尝试获取机加工线 (JJG) 的最新异常报告
		            OUTER APPLY (
		                SELECT TOP 1 halt_type 
		                FROM TB01_jjg_hour_analyse_report 
		                WHERE product_line_no = plhr.product_line_no 
		                  AND start_time = plhr.start_time 
		                ORDER BY qpr_sn DESC
		            ) harjjg
		
		            WHERE plhr.product_line_no = @LineNo
		              AND plhr.end_time <= @LastOffline
		              AND plhr.end_time >= @Start 
		              AND plhr.end_time < @End
		              AND plhr.start_time >= @Start -- 【修复点】：补齐异常开始时间的边界限制
		            GROUP BY ISNULL(har.halt_type, ISNULL(harjjg.halt_type, '异常停机'))";
		
		        using (SqlCommand cmd = new SqlCommand(sql, conn))
		        {
		            // 绑定基础坐标与时间轴参数
		            cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
		            cmd.Parameters.AddWithValue("@LastOffline", line.LastOfflineTime);
		            cmd.Parameters.AddWithValue("@Start", line.ShiftStartTime);
		            cmd.Parameters.AddWithValue("@End", line.ShiftEndTime);
		            
		            conn.Open();
		            using (SqlDataReader reader = cmd.ExecuteReader())
		            {
		                while (reader.Read())
		                {
		                    string haltType = reader["final_halt_type"].ToString();
		                    double diffMinutes = reader["total_diff"] == DBNull.Value ? 0 : Convert.ToDouble(reader["total_diff"]);
		                    
		                    // 将分钟转换为小时
		                    double diffHours = Math.Round(diffMinutes / 60.0, 2);
		
		                    if (haltType == "计划停机")
		                    {
		                        jgPlanStopTime += diffHours;
		                    }
		                    else
		                    {
		                        abnormalStop += diffHours;
		                    }
		                }
		            }
		        }
		    }
		    return abnormalStop;
		}
        private double GetTheoreticalCt(ProductLineOee line)
        {
            string safeProductNo = string.IsNullOrEmpty(line.ProductNo) ? "" : line.ProductNo;
            
          	using (SqlConnection conn = new SqlConnection(_mainDbConn))
		    {
		        string sql = @"SELECT MAX(ct_value) FROM TA05_product_line_product 
		                       WHERE product_line_no = @LineNo AND product_no = @ProdNo";
		        using (SqlCommand cmd = new SqlCommand(sql, conn))
		        {
		            cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
		            cmd.Parameters.AddWithValue("@ProdNo", safeProductNo);
		            conn.Open();
		            object result = cmd.ExecuteScalar();
		            return result == DBNull.Value || result == null ? 0 : Convert.ToDouble(result);
		        }
		    }
        }

        private double GetScrapQuantity(ProductLineOee line)
		{
		    using (SqlConnection conn = new SqlConnection(_mainDbConn))
		    {
		        // 核心修改：在 WHERE 子句末尾补充了针对装配线(ZP)和异常来源的联合判断
		        string sql = @"
		            SELECT SUM(CASE WHEN mnd.scrap_num > 0 THEN mnd.scrap_num ELSE mnd.consign_num END) as total_scrap
		            FROM T209_move_notify_detail mnd
		            INNER JOIN T209_move_notify_master mnm ON mnd.notify_no = mnm.notify_no
		            WHERE mnd.product_line_no = @LineNo
		            AND mnd.notify_type = 'unqualify_record'
		            AND mnd.abnormal_type_cn IN ('车间废','工厂废')
		            AND mnm.edit_date >= @Start AND mnm.edit_date < @End
		            AND mnm.abnormal_reason NOT LIKE '%金相切割%'
		            AND (@LineType <> 'ZP' OR (@LineType = 'ZP' AND mnd.source_type <> 'pb_lf_abnormal'))";
		
		        using (SqlCommand cmd = new SqlCommand(sql, conn))
		        {
		            // 绑定基础坐标与时间轴参数
		            cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
		            cmd.Parameters.AddWithValue("@Start", line.ShiftStartTime);
		            cmd.Parameters.AddWithValue("@End", line.ShiftEndTime);
		            
		            // 绑定新增的产线类型参数，做防御性处理防止空引用
		            cmd.Parameters.AddWithValue("@LineType", string.IsNullOrEmpty(line.ProductLineTypeNo) ? "" : line.ProductLineTypeNo);
		
		            conn.Open();
		            object result = cmd.ExecuteScalar();
		            return result == DBNull.Value || result == null ? 0 : Convert.ToDouble(result);
		        }
		    }
		}

       /// <summary>
/// 将 OEE 计算结果及所有相关的明细数据（异常、扫码区间）持久化到数据库
/// </summary>
/// <param name="lines">已完成 OEE 内存计算的产线集合</param>
/// <param name="date">目标计算日期</param>
/// <param name="lineScanMap">产线与其对应的扫码记录字典，用于生成区间汇总表</param>
		private void SaveOeeResultsToDatabase(List<ProductLineOee> lines, DateTime date, Dictionary<ProductLineOee, List<ScanRecord>> lineScanMap)
		{
		    using (SqlConnection conn = new SqlConnection(_mainDbConn))
		    {
		        conn.Open();
		        // 开启数据库事务机制，确保三张表的数据要么全部写入成功，要么全部回滚
		        using (SqlTransaction trans = conn.BeginTransaction())
		        {
		            try
		            {
		                // ========================================================
		                // 阶段一：数据幂等性清理（防止重复计算导致数据翻倍）
		                // ========================================================
		                
		                // 1. 清理主结果表的老旧数据
		                string delSql = "DELETE FROM TA07_shop_oee_calc_result WHERE start_date = @Date";
		                using (SqlCommand delCmd = new SqlCommand(delSql, conn, trans))
		                {
		                    delCmd.Parameters.AddWithValue("@Date", date.Date);
		                    delCmd.ExecuteNonQuery();
		                }
		
		                // 2. 清理异常明细表的历史数据
		                string delAbnormalSql = "DELETE FROM TA07_shop_oee_calc_abnormal WHERE input_date = @Date";
		                using (SqlCommand delAbCmd = new SqlCommand(delAbnormalSql, conn, trans))
		                {
		                    delAbCmd.Parameters.AddWithValue("@Date", date.Date);
		                    delAbCmd.ExecuteNonQuery();
		                }
		
		                // 3. 清理扫码计件区间表的历史数据
		                string delScanSql = "DELETE FROM TA07_shop_oee_calc_scan WHERE start_date = @Date";
		                using (SqlCommand delScanCmd = new SqlCommand(delScanSql, conn, trans))
		                {
		                    delScanCmd.Parameters.AddWithValue("@Date", date.Date);
		                    delScanCmd.ExecuteNonQuery();
		                }
		
		                // ========================================================
		                // 阶段二：遍历产线，将最新快照与明细写入各表
		                // ========================================================
		                
		                // 预定义主表的 Insert SQL，提升代码可读性
		                string insResultSql = @"INSERT INTO TA07_shop_oee_calc_result 
		                    (sys_no, product_line_no, start_date, start_time, end_time, total_time, rest,
		                    can_use_time, plan_stop_time, abnormal_stop_time, real_run_time, availability_rate,
		                    produced_quantity, real_ct_value, ct_value, performance_rate, quality_num, 
		                    scrap_num, quality_rate, oee_rate, product_no, jg_plan_stop_time)
		                    VALUES 
		                    (@SysNo, @LineNo, @StartDate, @StartTime, @EndTime, @TotalTime, @RestTime,
		                    @CanUse, @PlanStop, @AbnormalStop, @RunTime, @AvaRate,
		                    @ProdQty, @RealCt, @Ct, @PerfRate, @DirectPass, 
		                    @Scrap, @QualRate, @Oee, @ProdNo, @JgPlanStop)";
		
		                foreach (var line in lines)
		                {
		                    // --------------------------------------------------------
		                    // 2.1 写入 OEE 主表 (TA07_shop_oee_calc_result)
		                    // --------------------------------------------------------
		                    using (SqlCommand insCmd = new SqlCommand(insResultSql, conn, trans))
		                    {
		                        insCmd.Parameters.AddWithValue("@SysNo", line.SysNo ?? string.Empty);
		                        insCmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo ?? string.Empty);
		                        insCmd.Parameters.AddWithValue("@StartDate", line.ShiftStartDate);
		                        insCmd.Parameters.AddWithValue("@StartTime", line.ShiftStartTime);
		                        insCmd.Parameters.AddWithValue("@EndTime", line.LastOfflineTime);
		                        insCmd.Parameters.AddWithValue("@TotalTime", line.TotalTime);
		                        insCmd.Parameters.AddWithValue("@RestTime", line.RestTime);
		                        insCmd.Parameters.AddWithValue("@CanUse", line.CanUseTime);
		                        insCmd.Parameters.AddWithValue("@PlanStop", line.PlanStopTime);
		                        insCmd.Parameters.AddWithValue("@AbnormalStop", line.AbnormalStopTime);
		                        insCmd.Parameters.AddWithValue("@RunTime", line.RunTime);
		                        insCmd.Parameters.AddWithValue("@AvaRate", line.AvailabilityRate * 100); 
		                        insCmd.Parameters.AddWithValue("@ProdQty", line.ProducedQuantity);
		                        insCmd.Parameters.AddWithValue("@RealCt", line.RealCtValue);
		                        insCmd.Parameters.AddWithValue("@Ct", line.CtValue);
		                        insCmd.Parameters.AddWithValue("@PerfRate", line.PerformanceRate * 100);
		                        insCmd.Parameters.AddWithValue("@DirectPass", line.DirectPassParts);
		                        insCmd.Parameters.AddWithValue("@Scrap", line.ScrapNum);
		                        insCmd.Parameters.AddWithValue("@QualRate", line.QualityRate * 100);
		                        insCmd.Parameters.AddWithValue("@Oee", line.OeeRate);
		                        insCmd.Parameters.AddWithValue("@ProdNo", line.ProductNo ?? string.Empty);
		                        insCmd.Parameters.AddWithValue("@JgPlanStop", line.JgPlanStopTime);
		
		                        insCmd.ExecuteNonQuery();
		                    }
		
		                    // --------------------------------------------------------
		                    // 2.2 写入异常明细表 (TA07_shop_oee_calc_abnormal)
		                    // --------------------------------------------------------
		                    
		                    // A. 从车间小时报表提取并转换异常记录（使用 OUTER APPLY 关联最新状态）
		                    string insAbnormalHourSql = @"
		                        INSERT INTO TA07_shop_oee_calc_abnormal(
		                            sys_no, abnormal_guid, input_date, qpr_no, qpr_sn, product_line_no, hour_name,
		                            problem_type, problem_description, problem_analysis, problem_measure, level_tag, 
		                            test_worker_no, test_dept_no, finish_date, state_type, analyse_worker_no, analyse_date, 
		                            edit_user_no, edit_date, start_time, end_time, loss_type, active_name_cn, 
		                            loss_type_child, problem_analysis_type, diff_time, halt_type, andon_tag
		                        )
		                        SELECT 
		                            @SysNo, NEWID(), @StartDate, ISNULL(main.qpr_no,''), ISNULL(main.qpr_sn,1), @LineNo, ISNULL(main.hour_name,''),
		                            ISNULL(main.problem_type,''), ISNULL(main.problem_description,''), ISNULL(main.problem_analysis,''), ISNULL(main.problem_measure,''), 
		                            ISNULL(main.level_tag,'F'), ISNULL(main.test_worker_no,''), ISNULL(main.test_dept_no,''), ISNULL(main.finish_date,'1900-01-01'),
		                            ISNULL(main.state_type,''), ISNULL(main.analyse_worker_no,''), ISNULL(main.analyse_date,'1900-01-01'),
		                            ISNULL(main.edit_user_no,''), ISNULL(main.edit_date,'1900-01-01'), ISNULL(plhr.start_time,'1900-01-01'), ISNULL(plhr.end_time,'1900-01-01'), 
		                            ISNULL(har.loss_type, ISNULL(harjjg.loss_type, '维修')), ISNULL(main.active_name_cn,''), 
		                            ISNULL(har.loss_type_child, ISNULL(harjjg.loss_type_child, '')), ISNULL(main.problem_analysis_type,''), plhr.diff_time, 
		                            ISNULL(har.halt_type, ISNULL(harjjg.halt_type, '异常停机')), 'F'
		                        FROM T200_product_line_hour_diff_result plhr
		                        OUTER APPLY (
		                            SELECT TOP 1 * FROM TB01_zp_hour_analyse_report har 
		                            WHERE har.product_line_no = plhr.product_line_no AND har.start_time = plhr.start_time ORDER BY har.qpr_sn DESC
		                        ) har
		                        OUTER APPLY (
		                            SELECT TOP 1 * FROM TB01_jjg_hour_analyse_report harjjg 
		                            WHERE harjjg.product_line_no = plhr.product_line_no AND harjjg.start_time = plhr.start_time ORDER BY harjjg.qpr_sn DESC
		                        ) harjjg
		                        OUTER APPLY (
		                            SELECT TOP 1 * FROM VB01_hour_analyse_report main 
		                            WHERE main.product_line_no = plhr.product_line_no AND main.start_time = plhr.start_time ORDER BY main.qpr_sn DESC
		                        ) main
		                        WHERE plhr.product_line_no = @LineNo
		                          AND plhr.end_time <= @LastOffline
		                          AND plhr.end_time >= @StartTime 
		                          AND plhr.end_time < @EndTime
		                          AND plhr.start_time >= @StartTime";
		
		                    using (SqlCommand cmdHour = new SqlCommand(insAbnormalHourSql, conn, trans))
		                    {
		                        cmdHour.Parameters.AddWithValue("@SysNo", line.SysNo ?? "");
		                        cmdHour.Parameters.AddWithValue("@LineNo", line.ProductLineNo ?? "");
		                        cmdHour.Parameters.AddWithValue("@StartDate", line.ShiftStartDate);
		                        cmdHour.Parameters.AddWithValue("@StartTime", line.ShiftStartTime);
		                        cmdHour.Parameters.AddWithValue("@EndTime", line.ShiftEndTime);
		                        cmdHour.Parameters.AddWithValue("@LastOffline", line.LastOfflineTime);
		                        cmdHour.ExecuteNonQuery();
		                    }
		
		                    // B. 从安灯系统 (Andon) 提取实时异常呼叫记录
		                    string insAbnormalAndonSql = @"
		                        INSERT INTO TA07_shop_oee_calc_abnormal(
		                            sys_no, abnormal_guid, input_date, qpr_no, qpr_sn, product_line_no, hour_name,
		                            problem_type, problem_description, problem_analysis, problem_measure, level_tag, 
		                            test_worker_no, test_dept_no, finish_date, state_type, analyse_worker_no, analyse_date, 
		                            edit_user_no, edit_date, start_time, end_time, loss_type, active_name_cn, 
		                            loss_type_child, problem_analysis_type, diff_time, halt_type, andon_tag
		                        )
		                        SELECT  
		                            @SysNo, NEWID(), @StartDate, acd.collect_no, 0, acd.product_line_no, '',
		                            acd.scrap_type, acd.condition_note, acd.confirm_note, '', '', '', '', acd.confirm_date,
		                            acd.state_type, acd.cancel_worker_no, acd.cancel_date, '', acd.input_date, acd.input_date, acd.confirm_date, 
		                            '', '', '', '', 
		                            CASE WHEN acd.confirm_date > acd.input_date AND acd.confirm_tag = 'T'  
		                                 THEN ROUND(DATEDIFF(MINUTE, acd.input_date, acd.confirm_date), 2)
		                                 WHEN acd.confirm_date < acd.input_date AND acd.confirm_tag = 'F'  
		                                 THEN ROUND(DATEDIFF(MINUTE, acd.input_date, GETDATE()), 2)
		                                 ELSE 0 END, 
		                            '', 'T'
		                        FROM T200_andon_collect_detail acd 
		                        WHERE acd.product_line_no = @LineNo
		                          AND acd.collect_type IN ('EQ','Other')
		                          AND acd.abnormal_event_no <> 'QT_020'
		                          AND acd.input_date >= @StartTime 
		                          AND acd.input_date < @EndTime";
		
		                    using (SqlCommand cmdAndon = new SqlCommand(insAbnormalAndonSql, conn, trans))
		                    {
		                        cmdAndon.Parameters.AddWithValue("@SysNo", line.SysNo ?? "");
		                        cmdAndon.Parameters.AddWithValue("@LineNo", line.ProductLineNo ?? "");
		                        cmdAndon.Parameters.AddWithValue("@StartDate", line.ShiftStartDate);
		                        cmdAndon.Parameters.AddWithValue("@StartTime", line.ShiftStartTime);
		                        cmdAndon.Parameters.AddWithValue("@EndTime", line.ShiftEndTime);
		                        cmdAndon.ExecuteNonQuery();
		                    }
		
		                    // --------------------------------------------------------
		                    // 2.3 写入扫码计件区间汇总表 (TA07_shop_oee_calc_scan)
		                    // --------------------------------------------------------
		                    
		                    if (lineScanMap.ContainsKey(line) && lineScanMap[line].Any())
		                    {
		                        // 核心操作：通过 LINQ 在内存中对时间戳进行“抹零归整”，划分为精确到小时的区间
		                        var scanGroupSummary = lineScanMap[line]
		                            .GroupBy(r => new DateTime(r.OccurDate.Year, r.OccurDate.Month, r.OccurDate.Day, r.OccurDate.Hour, 0, 0))
		                            .Select(g => new
		                            {
		                                // 区间起点（例如：2025-02-10 08:00:00）
		                                StartTime = g.Key,
		                                // 区间终点（例如：2025-02-10 09:00:00）
		                                EndTime = g.Key.AddHours(1),
		                                // 预先格式化的区间名称（如："08:00~09:00"），减少前端图表渲染时的解析压力
		                                ResultName = g.Key.ToString("HH:mm") + "~" + g.Key.AddHours(1).ToString("HH:mm"),
		                                // 该时间窗口内的总下线产量
		                                ResultNum = g.Sum(x => x.DeliveryNum)
		                            }).ToList();
		
		                        string insScanSql = @"
		                            INSERT INTO TA07_shop_oee_calc_scan(
		                                sys_no, scan_guid, product_line_no, start_date, 
		                                result_num, result_name, start_time, end_time
		                            ) VALUES (
		                                @SysNo, NEWID(), @LineNo, @StartDate, 
		                                @ResultNum, @ResultName, @StartTime, @EndTime
		                            )";
		
		                        foreach (var group in scanGroupSummary)
		                        {
		                            using (SqlCommand cmdScan = new SqlCommand(insScanSql, conn, trans))
		                            {
		                                // NEWID() 由数据库隐式调用以生成绝对不冲突的 GUID 记录主键
		                                cmdScan.Parameters.AddWithValue("@SysNo", line.SysNo ?? "");
		                                cmdScan.Parameters.AddWithValue("@LineNo", line.ProductLineNo ?? "");
		                                cmdScan.Parameters.AddWithValue("@StartDate", line.ShiftStartDate);
		                                cmdScan.Parameters.AddWithValue("@ResultNum", group.ResultNum);
		                                cmdScan.Parameters.AddWithValue("@ResultName", group.ResultName);
		                                cmdScan.Parameters.AddWithValue("@StartTime", group.StartTime);
		                                cmdScan.Parameters.AddWithValue("@EndTime", group.EndTime);
		                                
		                                cmdScan.ExecuteNonQuery();
		                            }
		                        }
		                    }
		                } // 结束 foreach 循环
		
		                // 当主表、异常表、扫码表均无错误时，统一提交事务，数据正式落盘
		                trans.Commit();
		            }
		            catch
		            {
		                // 一旦发生异常（例如断网或锁表），立刻回滚所有操作，避免出现“半个班次的数据”
		                trans.Rollback();
		                throw; 
		            }
		        }
		    }
		}
    }
    // 内部协助类：用于承载数采库的扫码记录载体
    internal class ScanRecord
    {
        public DateTime OccurDate { get; set; }
        public string ProductNo { get; set; }
        public double DeliveryNum { get; set; }
    }
}
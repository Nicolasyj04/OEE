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

            foreach (var line in productLines)
            {
                line.SysNo = sysNo;

                // 2. 跨库查询：连接数采数据库获取该产线的实时扫码记录
                var scanRecords = GetScanRecords(line);

                if (scanRecords.Any())
                {
                    line.FirstOfflineTime = scanRecords.Min(s => s.OccurDate);
                    line.LastOfflineTime = scanRecords.Max(s => s.OccurDate);
                    
                    // 获取最近一次下线的产品型号
                    var lastScan = scanRecords.OrderByDescending(s => s.OccurDate).FirstOrDefault();
                    if (lastScan != null)
                    {
                        line.ProductNo = lastScan.ProductNo;
                    }
                    
                    line.ProducedQuantity = scanRecords.Sum(s => s.DeliveryNum);
                }
                else
                {
                    // 若无扫码记录，初始化基础默认时间（对应原 SQL 的 '1900-01-01' 处理）
                    line.FirstOfflineTime = new DateTime(1900, 1, 1);
                    line.LastOfflineTime = new DateTime(1900, 1, 1);
                }

                // 3. 从主库拉取各项耗时与异常参数
                line.RestTime = GetRestTime(line);
                line.PlanStopTime = GetPlanStopTime(line);
                // 第一步：先声明一个局部变量（准备好空盒子）
				double jgPlanStop; 
				
				// 第二步：使用 out 关键字将它传进去（注意，这里只有 out 和变量名，没有类型定义了）
				line.AbnormalStopTime = GetAbnormalStopTime(line, out jgPlanStop);
                line.JgPlanStopTime = jgPlanStop; // 间隔报表的计划停机

                // 获取理论节拍(CT)和不合格报废数
                line.CtValue = GetTheoreticalCt(line);
                line.ScrapNum = GetScrapQuantity(line);

                // 4. 执行内存级数学运算 (取代原 SQL 中繁杂的十几个 Update 语句)
                ComputeOeeMath(line);
            }

            // 5. 将计算结果持久化（保存）到主数据库的业务表中
            SaveOeeResultsToDatabase(productLines, targetDate);
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
            // 注意：这里使用的是 _collectDbConn，实现了物理上的数据库分离
            using (SqlConnection conn = new SqlConnection(_collectDbConn))
            {
                // 联合查询 Vx02_scan_record_last_op 和 tx02_assm_offline_record 替代原存储过程的复杂拼接
                string sql = string.Empty;
                if (line.ProductLineTypeNo != "ZP")
                {
                    sql = @"SELECT edit_time as occur_date, product_no, cast(1 as float) as delivery_num 
                            FROM datacollect_base.dbo.Vx02_scan_record_last_op 
                            WHERE product_line_no = @LineNo AND edit_time >= @Start AND edit_time < @End";
                }
                else
                {
                    sql = @"SELECT offline_time as occur_date, '' as product_no, cast(1 as float) as delivery_num 
                            FROM datacollect_base.dbo.tx02_assm_offline_record 
                            WHERE product_line_no = @LineNo AND offline_time >= @Start AND offline_time < @End";
                }

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
                    cmd.Parameters.AddWithValue("@Start", line.ShiftStartTime);
                    cmd.Parameters.AddWithValue("@End", line.ShiftEndTime);
                    conn.Open();
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            records.Add(new ScanRecord
                            {
                                OccurDate = Convert.ToDateTime(reader["occur_date"]),
                                ProductNo = reader["product_no"].ToString(),
                                DeliveryNum = Convert.ToDouble(reader["delivery_num"])
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
                // 注意：在实际工程中，复杂的联合统计通常在视图(View)中完成，然后在 C# 中简单查询。
                // 这里为了忠实还原原 SQL 逻辑，直接拉取明细进行求和计算。
                string sql = @"
                    SELECT SUM(plhr.diff_time) as total_diff, har.halt_type
                    FROM T200_product_line_hour_diff_result plhr
                    LEFT JOIN TB01_zp_hour_analyse_report har 
                         ON har.product_line_no = plhr.product_line_no AND har.start_time = plhr.start_time
                    WHERE plhr.product_line_no = @LineNo
                    AND plhr.end_time <= @LastOffline
                    AND plhr.end_time >= @Start AND plhr.end_time < @End
                    GROUP BY har.halt_type";

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
                    cmd.Parameters.AddWithValue("@LastOffline", line.LastOfflineTime);
                    cmd.Parameters.AddWithValue("@Start", line.ShiftStartTime);
                    cmd.Parameters.AddWithValue("@End", line.ShiftEndTime);
                    
                    conn.Open();
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string haltType = reader["halt_type"] == DBNull.Value ? "异常停机" : reader["halt_type"].ToString();
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
            if (string.IsNullOrEmpty(line.ProductNo)) return 0;
            
            using (SqlConnection conn = new SqlConnection(_mainDbConn))
            {
                string sql = @"SELECT MAX(ct_value) FROM TA05_product_line_product 
                               WHERE product_line_no = @LineNo AND product_no = @ProdNo";
                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
                    cmd.Parameters.AddWithValue("@ProdNo", line.ProductNo);
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
                string sql = @"
                    SELECT SUM(CASE WHEN mnd.scrap_num > 0 THEN mnd.scrap_num ELSE mnd.consign_num END) as total_scrap
                    FROM T209_move_notify_detail mnd
                    INNER JOIN T209_move_notify_master mnm ON mnd.notify_no = mnm.notify_no
                    WHERE mnd.product_line_no = @LineNo
                    AND mnd.notify_type = 'unqualify_record'
                    AND mnd.abnormal_type_cn IN ('车间废','工厂废')
                    AND mnm.edit_date >= @Start AND mnm.edit_date < @End
                    AND mnm.abnormal_reason NOT LIKE '%金相切割%'";

                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@LineNo", line.ProductLineNo);
                    cmd.Parameters.AddWithValue("@Start", line.ShiftStartTime);
                    cmd.Parameters.AddWithValue("@End", line.ShiftEndTime);
                    conn.Open();
                    object result = cmd.ExecuteScalar();
                    return result == DBNull.Value || result == null ? 0 : Convert.ToDouble(result);
                }
            }
        }

        private void SaveOeeResultsToDatabase(List<ProductLineOee> lines, DateTime date)
        {
            using (SqlConnection conn = new SqlConnection(_mainDbConn))
            {
                conn.Open();
                // 开启数据库事务机制
                using (SqlTransaction trans = conn.BeginTransaction())
                {
                    try
                    {
                        // 阶段一：清理由于重复计算带来的老旧历史数据，确保幂等性。
                        string delSql = "DELETE FROM TA07_shop_oee_calc_result WHERE start_date = @Date";
                        using (SqlCommand delCmd = new SqlCommand(delSql, conn, trans))
                        {
                            delCmd.Parameters.AddWithValue("@Date", date.Date);
                            delCmd.ExecuteNonQuery();
                        }

                        // 阶段二：使用参数化查询写入新一轮的计算快照
                        string insSql = @"INSERT INTO TA07_shop_oee_calc_result 
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
                            using (SqlCommand insCmd = new SqlCommand(insSql, conn, trans))
                            {
                                // 通过 AddWithValue 将对象的属性映射到 SQL 的占位符上
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
                        }

                        // 如果循环内所有命令均未出错，则将所有更改一并提交至数据库生效
                        trans.Commit();
                    }
                    catch
                    {
                        // 一旦发生网络闪断或约束冲突，执行回滚，保障数据库的一致性
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
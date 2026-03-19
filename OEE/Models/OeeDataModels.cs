using System;

namespace OEE.Models
{
    /// <summary>
    /// 产线 OEE 数据核心实体模型 (POCO)
    /// </summary>
    public class ProductLineOee
    {
        // ==========================================
        // 第一部分：基础定义与身份参数
        // 理论意义：用于在数据库中唯一确定一条记录的“坐标”。
        // ==========================================
        
        /// <summary>
        /// 厂区或系统编号 (如 "660" 代表某个特定厂区)
        /// </summary>
        public string SysNo { get; set; }
        
        /// <summary>
        /// 产线编号 (物理设备的唯一标识，如 "Line_A1")
        /// </summary>
        public string ProductLineNo { get; set; }
        
        /// <summary>
        /// 产线类型 (用于区分不同工艺，如 "ZP" 代表装配线)
        /// 实际应用：不同类型的产线，采集下线数据的数据源往往不同。
        /// </summary>
        public string ProductLineTypeNo { get; set; }
        
        /// <summary>
        /// 产品型号 (当前产线正在生产的具体产品料号)
        /// </summary>
        public string ProductNo { get; set; }

        // ==========================================
        // 第二部分：时间轴参数
        // 理论意义：OEE 的核心是“时间管理”，必须精准划定计算的时间边界。
        // ==========================================
        
        /// <summary>
        /// 归属日期 (该班次在统计上属于哪一天，去除了时分秒)
        /// </summary>
        public DateTime ShiftStartDate { get; set; }
        
        /// <summary>
        /// 班次物理开始时间 (如白班的早上 08:00:00)
        /// </summary>
        public DateTime ShiftStartTime { get; set; }
        
        /// <summary>
        /// 班次物理结束时间 (如白班的晚上 20:00:00，包含跨天逻辑)
        /// </summary>
        public DateTime ShiftEndTime { get; set; }
        
        /// <summary>
        /// 首件下线时间 (产线今天吐出的第一个合格品的时间)
        /// </summary>
        public DateTime FirstOfflineTime { get; set; }
        
        /// <summary>
        /// 末件下线时间 (当前获取到的最新一个产品下线的时间)
        /// </summary>
        public DateTime LastOfflineTime { get; set; }

        // ==========================================
        // 第三部分：OEE 时间分解参数 (单位：小时)
        // 理论意义：将员工在厂时间一步步剥离，找到真正用于创造价值的时间。
        // ==========================================
        
        /// <summary>
        /// 出勤时间 (TotalTime)
        /// 实际应用：理论上员工打卡在岗的总时间。算法上通常是“末件下线时间 - 班次开始时间”。
        /// </summary>
        public double TotalTime { get; set; } 
        
        /// <summary>
        /// 休息时间 (RestTime)
        /// 实际应用：如中午吃饭 1 小时，属于不排产的无效时间，在计算可用时间时要扣除。
        /// </summary>
        public double RestTime { get; set; }  
        
        /// <summary>
        /// 计划停机时间 (PlanStopTime)
        /// 实际应用：如每周三下午 2 点进行设备大保养，这种停机不认为是设备效率低下，所以从考核基数中扣除。
        /// </summary>
        public double PlanStopTime { get; set; } 
        
        /// <summary>
        /// 间隔计划停机时间 (JgPlanStopTime)
        /// 实际应用：车间现场通过安灯(Andon)或报表手动上报的、穿插在生产过程中的小段计划内停机。
        /// </summary>
        public double JgPlanStopTime { get; set; }
        
        /// <summary>
        /// 异常停机时间 (AbnormalStopTime)
        /// 实际应用：真正的“效率杀手”，如设备突然断电、机械臂卡死、仓库缺料等待。直接拉低时间稼动率。
        /// </summary>
        public double AbnormalStopTime { get; set; } 

        /// <summary>
        /// 可用时间 (CanUseTime = TotalTime - RestTime)
        /// </summary>
        public double CanUseTime { get; set; }
        
        /// <summary>
        /// 实际运行时间 (RunTime = CanUseTime - PlanStopTime - AbnormalStopTime)
        /// 实际应用：设备真正在轰鸣运转、制造产品的时间总和。
        /// </summary>
        public double RunTime { get; set; }

        // ==========================================
        // 第四部分：生产成果与节拍参数
        // 理论意义：评估设备跑得快不快、好不好。
        // ==========================================
        
        /// <summary>
        /// 实际总产量 (包含良品和废品)
        /// </summary>
        public double ProducedQuantity { get; set; } 
        
        /// <summary>
        /// 报废数 (不合格品数量)
        /// </summary>
        public double ScrapNum { get; set; }         
        
        /// <summary>
        /// 直接合格数 (良品数 = 总产量 - 报废数)
        /// </summary>
        public double DirectPassParts { get; set; }  
        
        /// <summary>
        /// 理论节拍 (Cycle Time - CT, 单位：秒)
        /// 实际应用：工艺工程师设定的标准。比如设定 60 秒必须产出一个。
        /// </summary>
        public double CtValue { get; set; }          
        
        /// <summary>
        /// 实际节拍 (Real Cycle Time, 单位：秒)
        /// 实际应用：实际运行时间 / 总产量。如果实际节拍变成 80 秒，说明设备在“降速/怠速”运行。
        /// </summary>
        public double RealCtValue { get; set; }      

        // ==========================================
        // 第五部分：OEE 终极考核指标 (0.00 ~ 1.00 之间的百分比)
        // ==========================================
        
        /// <summary>
        /// 时间稼动率 (Availability Rate)
        /// 评估停机损失。公式：实际运行时间 / 可用时间
        /// </summary>
        public double AvailabilityRate { get; set; }
        
        /// <summary>
        /// 性能稼动率 (Performance Rate)
        /// 评估速度损失。公式：理论节拍 / 实际节拍
        /// </summary>
        public double PerformanceRate { get; set; }
        
        /// <summary>
        /// 良品率 (Quality Rate)
        /// 评估质量损失。公式：合格数 / 总产量
        /// </summary>
        public double QualityRate { get; set; }
        
        /// <summary>
        /// 综合设备效率 (OEE = 时间稼动率 × 性能稼动率 × 良品率)
        /// 实际应用：存入数据库时通常乘以 100 转化为直观的百分比数值（如 85.5% 存为 85.5）。
        /// </summary>
        public double OeeRate { get; set; } 
    }
}
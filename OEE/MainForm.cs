using System;
using System.Drawing;
using System.Threading.Tasks;
using System.Windows.Forms;
using OEE.Models;
using OEE.Services; // 引入我们之前的业务逻辑层

namespace OEE
{
    public partial class MainForm : Form
    {
        // 声明界面控件
        private Label lblSysNo;
        private TextBox txtSysNo;
        
        private Label lblTargetDate;
        private DateTimePicker dtpTargetDate;
        
        private Label lblProductLine;
        private TextBox txtProductLine;
        
        private Button btnCalculate;
        private RichTextBox rtbResult;

        // 实例化业务逻辑服务
        private readonly OeeCalculatorService _oeeService;

       public MainForm()
        {
            // 如果你的项目中原本有 InitializeComponent(); 可以保留它，
            // 并把我们手写的 InitializeUI(); 放在它下面。
            // InitializeComponent(); 
            
            // 初始化窗体基本属性
            this.Text = "产线 OEE 实时计算工具";
            this.Size = new Size(450, 350);
            this.StartPosition = FormStartPosition.CenterScreen;

            _oeeService = new OeeCalculatorService();

            InitializeUI();
        }

        /// <summary>
        /// 初始化和布局界面控件
        /// </summary>
        private void InitializeUI()
        {
            // 1. 系统编号输入区
            lblSysNo = new Label() { Text = "系统编号:", Location = new Point(30, 30), AutoSize = true };
            txtSysNo = new TextBox() { Location = new Point(120, 26), Width = 150, Text = "660" };
            
            // 2. 目标日期选择区
            lblTargetDate = new Label() { Text = "目标日期:", Location = new Point(30, 70), AutoSize = true };
            dtpTargetDate = new DateTimePicker() 
            { 
                Location = new Point(120, 66), 
                Width = 150,
                Format = DateTimePickerFormat.Short // 仅显示年月日
            };

            // 3. 产线编号输入区
            lblProductLine = new Label() { Text = "产线编号:", Location = new Point(30, 110), AutoSize = true };
            txtProductLine = new TextBox() { Location = new Point(120, 106), Width = 150 };
            
            // 4. 执行按钮
            btnCalculate = new Button() 
            { 
                Text = "执行 OEE 计算", 
                Location = new Point(120, 150), 
                Width = 150,
                Height = 35,
                BackColor = Color.LightBlue
            };
            // 绑定点击事件委托
            btnCalculate.Click += BtnCalculate_Click;

            // 5. 结果输出文本框
            rtbResult = new RichTextBox() 
            { 
                Location = new Point(30, 200), 
                Width = 370, 
                Height = 80,
                ReadOnly = true // 设为只读，仅用于显示信息
            };

            // 将控件添加到窗体的控件集合中
            this.Controls.Add(lblSysNo);
            this.Controls.Add(txtSysNo);
            this.Controls.Add(lblTargetDate);
            this.Controls.Add(dtpTargetDate);
            this.Controls.Add(lblProductLine);
            this.Controls.Add(txtProductLine);
            this.Controls.Add(btnCalculate);
            this.Controls.Add(rtbResult);
        }

        /// <summary>
        /// 按钮点击事件的处理逻辑
        /// </summary>
        /// <param name="sender">触发事件的控件对象（即 btnCalculate）</param>
        /// <param name="e">包含事件数据的对象</param>
        private async void BtnCalculate_Click(object sender, EventArgs e)
        {
            // UI 交互优化：禁用按钮防止重复点击
            btnCalculate.Enabled = false;
            rtbResult.Clear();
            rtbResult.SelectionColor = Color.Blue;
           rtbResult.AppendText(string.Format("[{0:HH:mm:ss}] 正在跨库拉取数据并计算，请稍候...\n", DateTime.Now));

            // 收集前端界面的参数
            string sysNo = txtSysNo.Text.Trim();
            // dtpTargetDate.Value 返回的是包含时分秒的 DateTime，使用 .Date 属性确保只取日期部分 (00:00:00)
            DateTime targetDate = dtpTargetDate.Value.Date; 
            string productLineNo = txtProductLine.Text.Trim();

            try
            {
                // 实际应用中，数据库操作可能比较耗时。
                // 使用 Task.Run 将计算逻辑放入后台线程执行，避免 WinForm 主界面（UI线程）假死卡顿。
                await Task.Run(() => 
                {
                    _oeeService.CalculateAndSaveOee(sysNo, targetDate, productLineNo);
                });

                rtbResult.SelectionColor = Color.Green;
                rtbResult.AppendText(string.Format("[{0:HH:mm:ss}] OEE 计算并保存成功！\n", DateTime.Now));
            }
            catch (Exception ex)
            {
                rtbResult.SelectionColor = Color.Red;
                // ex.Message 包含了底层抛出的具体异常文本
                rtbResult.AppendText(string.Format("[{0:HH:mm:ss}] 计算发生异常: {1}\n", DateTime.Now, ex.Message));
                
                // 可选：使用 MessageBox 弹窗强提示
                // MessageBox.Show(ex.Message, "错误", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            finally
            {
                // 无论成功还是失败，最后都要恢复按钮为可用状态
                btnCalculate.Enabled = true;
            }
        }
    }
}
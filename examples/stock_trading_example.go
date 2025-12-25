package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/warriorguo/workflow"
	"github.com/warriorguo/workflow/types"
)

// StockInfo 股票信息
type StockInfo struct {
	Code      string  // 股票代码
	Name      string  // 股票名称
	Price     float64 // 当前价格
	MarketCap float64 // 市值（亿元）
	PE        float64 // 市盈率
	Industry  string  // 所属行业
	IsST      bool    // 是否ST股票
	Suspended bool    // 是否停牌
	Delisted  bool    // 是否退市
}

// MockStockData 模拟股票数据
var MockStockData = map[string]*StockInfo{
	"600000": {
		Code:      "600000",
		Name:      "浦发银行",
		Price:     8.50,
		MarketCap: 2500.0,
		PE:        5.2,
		Industry:  "银行",
		IsST:      false,
	},
	"000001": {
		Code:      "000001",
		Name:      "平安银行",
		Price:     12.30,
		MarketCap: 2380.0,
		PE:        6.8,
		Industry:  "银行",
		IsST:      false,
	},
	"300059": {
		Code:      "300059",
		Name:      "东方财富",
		Price:     15.80,
		MarketCap: 2450.0,
		PE:        45.6,
		Industry:  "互联网金融",
		IsST:      false,
	},
	"600519": {
		Code:      "600519",
		Name:      "贵州茅台",
		Price:     1680.50,
		MarketCap: 21000.0,
		PE:        35.8,
		Industry:  "白酒",
		IsST:      false,
	},
	"688981": {
		Code:      "688981",
		Name:      "中芯国际",
		Price:     42.50,
		MarketCap: 3380.0,
		PE:        85.6,
		Industry:  "半导体",
		IsST:      false,
	},
	"300750": {
		Code:      "300750",
		Name:      "宁德时代",
		Price:     168.50,
		MarketCap: 39000.0,
		PE:        55.2,
		Industry:  "新能源",
		IsST:      false,
	},
	"000063": {
		Code:      "000063",
		Name:      "*ST中兴",
		Price:     0.85,
		MarketCap: 35.0,
		PE:        -15.2,
		Industry:  "通信设备",
		IsST:      true,
	},
	"002153": {
		Code:      "002153",
		Name:      "石基信息",
		Price:     28.60,
		MarketCap: 185.0,
		PE:        125.8,
		Industry:  "软件服务",
		IsST:      false,
	},
	"688599": {
		Code:      "688599",
		Name:      "天合光能",
		Price:     32.80,
		MarketCap: 558.0,
		PE:        42.5,
		Industry:  "光伏设备",
		IsST:      false,
	},
}

// 科技相关行业列表
var TechIndustries = map[string]bool{
	"半导体":   true,
	"软件服务":  true,
	"计算机设备": true,
	"通信设备":  true,
	"光伏设备":  true,
	"新能源":   true,
	"人工智能":  true,
	"云计算":   true,
	"大数据":   true,
	"物联网":   true,
	"5G":    true,
}

// 获取股票信息节点
func getStockInfo(ctx types.Context, input types.Data) (types.Data, error) {
	stockCode, exists := input.GetString("stock_code")
	if !exists {
		return nil, fmt.Errorf("缺少股票代码参数")
	}

	fmt.Printf("📊 正在获取股票信息: %s\n", stockCode)

	// 从模拟数据中获取股票信息
	stockInfo, exists := MockStockData[stockCode]
	if !exists {
		return nil, fmt.Errorf("股票代码 %s 不存在", stockCode)
	}

	// 将股票信息保存到数据流中
	input.Set("stock_info", stockInfo)
	input.Set("stock_name", stockInfo.Name)
	input.Set("price", stockInfo.Price)
	input.Set("market_cap", stockInfo.MarketCap)
	input.Set("pe", stockInfo.PE)
	input.Set("industry", stockInfo.Industry)
	input.Set("is_st", stockInfo.IsST)

	fmt.Printf("   股票名称: %s\n", stockInfo.Name)
	fmt.Printf("   当前价格: %.2f 元\n", stockInfo.Price)
	fmt.Printf("   市值: %.2f 亿元\n", stockInfo.MarketCap)
	fmt.Printf("   市盈率: %.2f\n", stockInfo.PE)
	fmt.Printf("   所属行业: %s\n", stockInfo.Industry)
	fmt.Printf("   是否ST: %v\n", stockInfo.IsST)

	return input, nil
}

// 检查是否为ST股票
func checkNotST(ctx types.Context, input types.Data) (bool, error) {
	isST, _ := input.GetBool("is_st")

	fmt.Printf("\n🔍 检查是否ST股票...\n")
	if isST {
		fmt.Printf("   ❌ 该股票为ST股票，不建议购买\n")
		return false, nil
	}

	fmt.Printf("   ✅ 非ST股票，通过检查\n")
	return true, nil
}

// 检查价格是否>=1元
func checkPriceAboveOne(ctx types.Context, input types.Data) (bool, error) {
	price, _ := input.Get("price")
	priceFloat, ok := price.(float64)
	if !ok {
		return false, fmt.Errorf("价格数据格式错误")
	}

	fmt.Printf("\n🔍 检查价格是否 >= 1元...\n")
	if priceFloat < 1.0 {
		fmt.Printf("   ❌ 价格 %.2f 元 < 1元，不建议购买\n", priceFloat)
		return false, nil
	}

	fmt.Printf("   ✅ 价格 %.2f 元 >= 1元，通过检查\n", priceFloat)
	return true, nil
}

// 检查是否为科技相关行业
func checkIsTechIndustry(ctx types.Context, input types.Data) (bool, error) {
	industry, _ := input.GetString("industry")

	fmt.Printf("\n🔍 检查是否科技相关行业...\n")
	fmt.Printf("   行业: %s\n", industry)

	// 检查是否为科技行业
	isTech := false
	for techIndustry := range TechIndustries {
		if strings.Contains(industry, techIndustry) || industry == techIndustry {
			isTech = true
			break
		}
	}

	if !isTech {
		fmt.Printf("   ❌ %s 不属于科技相关行业，不建议购买\n", industry)
		return false, nil
	}

	fmt.Printf("   ✅ %s 属于科技相关行业，通过检查\n", industry)
	return true, nil
}

// 检查PE是否<=100
func checkPELessThan100(ctx types.Context, input types.Data) (bool, error) {
	pe, _ := input.Get("pe")
	peFloat, ok := pe.(float64)
	if !ok {
		return false, fmt.Errorf("PE数据格式错误")
	}

	fmt.Printf("\n🔍 检查PE是否 <= 100...\n")
	if peFloat > 100.0 {
		fmt.Printf("   ❌ PE %.2f > 100，估值过高，不建议购买\n", peFloat)
		return false, nil
	}

	fmt.Printf("   ✅ PE %.2f <= 100，估值合理，通过检查\n", peFloat)
	return true, nil
}

// 拒绝购买节点
func rejectPurchase(ctx types.Context, input types.Data) (types.Data, error) {
	stockName, _ := input.GetString("stock_name")
	reason, exists := input.GetString("reject_reason")
	if !exists {
		reason = "不满足购买条件"
	}

	fmt.Printf("\n" + strings.Repeat("=", 60) + "\n")
	fmt.Printf("🚫 决策结果: 不建议购买 %s\n", stockName)
	fmt.Printf("   原因: %s\n", reason)
	fmt.Printf(strings.Repeat("=", 60) + "\n\n")

	input.Set("decision", "REJECT")
	input.Set("decision_reason", reason)
	return input, nil
}

// 同意购买节点
func approvePurchase(ctx types.Context, input types.Data) (types.Data, error) {
	stockName, _ := input.GetString("stock_name")
	price, _ := input.Get("price")
	industry, _ := input.GetString("industry")
	pe, _ := input.Get("pe")

	fmt.Printf("\n" + strings.Repeat("=", 60) + "\n")
	fmt.Printf("✅ 决策结果: 建议购买 %s\n", stockName)
	fmt.Printf("   价格: %.2f 元\n", price)
	fmt.Printf("   行业: %s\n", industry)
	fmt.Printf("   PE: %.2f\n", pe)
	fmt.Printf("   该股票通过所有筛选条件，可以考虑购买\n")
	fmt.Printf(strings.Repeat("=", 60) + "\n\n")

	input.Set("decision", "APPROVE")
	input.Set("decision_reason", "通过所有筛选条件")
	return input, nil
}

// 设置拒绝原因的辅助节点
func setRejectReasonST(ctx types.Context, input types.Data) (types.Data, error) {
	input.Set("reject_reason", "该股票为ST股票")
	return input, nil
}

func setRejectReasonPrice(ctx types.Context, input types.Data) (types.Data, error) {
	input.Set("reject_reason", "价格低于1元")
	return input, nil
}

func setRejectReasonIndustry(ctx types.Context, input types.Data) (types.Data, error) {
	input.Set("reject_reason", "不属于科技相关行业")
	return input, nil
}

func setRejectReasonPE(ctx types.Context, input types.Data) (types.Data, error) {
	input.Set("reject_reason", "PE大于100，估值过高")
	return input, nil
}

// 构建股票买入决策工作流
func buildStockTradingWorkflow(dag types.DAG) error {
	// 第一步：创建所有普通节点
	// 1. 获取股票信息节点
	if err := dag.Node("get_stock_info", getStockInfo); err != nil {
		return err
	}

	// 2. 拒绝原因设置节点
	if err := dag.Node("reject_st", setRejectReasonST); err != nil {
		return err
	}

	if err := dag.Node("reject_price", setRejectReasonPrice); err != nil {
		return err
	}

	if err := dag.Node("reject_industry", setRejectReasonIndustry); err != nil {
		return err
	}

	if err := dag.Node("reject_pe", setRejectReasonPE); err != nil {
		return err
	}

	// 3. 最终决策节点
	if err := dag.Node("approve", approvePurchase); err != nil {
		return err
	}

	if err := dag.Node("reject", rejectPurchase); err != nil {
		return err
	}

	// PE检查
	if err := dag.Condition("check_pe", "approve", "reject_pe", checkPELessThan100); err != nil {
		return err
	}
	// 行业检查
	if err := dag.Condition("check_industry", "check_pe", "reject_industry", checkIsTechIndustry); err != nil {
		return err
	}
	// 价格检查
	if err := dag.Condition("check_price", "check_industry", "reject_price", checkPriceAboveOne); err != nil {
		return err
	}
	// 第二步：创建所有条件节点（在所有普通节点创建后）
	// ST股票检查
	if err := dag.Condition("check_not_st", "check_price", "reject_st", checkNotST); err != nil {
		return err
	}

	// 连接工作流
	// get_stock_info -> check_not_st
	if err := dag.Edge("get_stock_info", "check_not_st"); err != nil {
		return err
	}

	// 所有拒绝分支都指向reject节点
	if err := dag.Edge("reject_st", "reject"); err != nil {
		return err
	}
	if err := dag.Edge("reject_price", "reject"); err != nil {
		return err
	}
	if err := dag.Edge("reject_industry", "reject"); err != nil {
		return err
	}
	if err := dag.Edge("reject_pe", "reject"); err != nil {
		return err
	}

	return nil
}

func main() {
	// 创建工作流引擎（使用内存存储）
	engine, err := workflow.NewFlowEngine(
		types.EnableMemStore(),
		types.DisableAutoStart(), // 禁用自动启动，手动控制执行
	)
	if err != nil {
		log.Fatalf("创建工作流引擎失败: %v", err)
	}

	// 注册股票买入决策工作流
	err = engine.RegisterDAG("stock_trading_decision", buildStockTradingWorkflow)
	if err != nil {
		log.Fatalf("注册工作流失败: %v", err)
	}

	fmt.Println("=" + strings.Repeat("=", 70))
	fmt.Println("  股票买入决策系统 - 基于 Workflow 引擎")
	fmt.Println("=" + strings.Repeat("=", 70))
	fmt.Println()

	// 测试多个股票
	testStocks := []string{
		"688981", // 中芯国际 - 半导体，应该通过
		"000063", // *ST中兴 - ST股票，应该被拒绝
		"600519", // 贵州茅台 - 白酒行业，应该被拒绝
		"002153", // 石基信息 - PE>100，应该被拒绝
		"688599", // 天合光能 - 光伏设备，应该通过
		"300750", // 宁德时代 - 新能源，应该通过
		"000001", // 平安银行 - 银行业，应该被拒绝
	}

	ctx := context.Background()

	for i, stockCode := range testStocks {
		fmt.Printf("\n测试案例 %d: 分析股票 %s\n", i+1, stockCode)
		fmt.Println(strings.Repeat("-", 72))

		// 准备输入参数
		params := types.Data{}
		params.Set("stock_code", stockCode)

		// 运行工作流
		requestID := fmt.Sprintf("stock-decision-%s", stockCode)
		err = engine.RunDAG(ctx, "stock_trading_decision", requestID, params)
		if err != nil {
			log.Printf("执行工作流失败: %v", err)
			continue
		}

		// 手动执行工作流
		for {
			err := engine.RunOnce()
			if err != nil {
				log.Printf("执行步骤失败: %v", err)
				break
			}

			// 检查状态
			status, err := engine.GetRequestStatus(ctx, requestID)
			if err != nil {
				log.Printf("获取状态失败: %v", err)
				break
			}

			// 如果完成或失败，退出循环
			if status.Status == types.Finished || status.Status == types.Failed || status.Status == types.Fatal {
				break
			}
		}

		fmt.Println()
	}

	fmt.Println("\n" + strings.Repeat("=", 72))
	fmt.Println("所有测试完成!")
	fmt.Println(strings.Repeat("=", 72))
}

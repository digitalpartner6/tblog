package models

import(
    "fmt"
    "strconv"
    "math"
)

type Stats struct{
    Id string
    TotalProfit float64
    TotalLoss float64
    ProfitHand float64
    LossHand float64
    RetainedProfit float64
    TotalHand float64
    StayHand float64
    AvgRetained float64
    AvgProfit float64
    AvgLoss float64
    MaxProfit float64
    MaxLoss float64
    TotalBar float64
}

func SetStats(id string) (err error){
    // 总盈利, 盈利手数
    total_profit, profit_hand, err := ZongYingLi()
    if err != nil {
        return 
    }

    // 总亏损, 亏损手数 @todo 未算手续费
    total_loss, loss_hand, err := ZongKuiSun()
    if err != nil {
        return
    }

    //净利润
    retained_profit := total_profit - total_loss

    // 盈亏比
//    profitPloss := total_profit/total_loss

    // 总手数
    total_hand, err := ZongShouShu()
    if err != nil {
        return 
    }

    //盈利比率
//    profitPtotal_hand := profit_hand / total_hand

    // 持平手数
    stay_hand := total_hand - profit_hand - loss_hand

    // 平均利润
    avg_retained := retained_profit / total_hand

    // 平均盈利
    avg_profit := total_profit / profit_hand

    // 平均亏损
    avg_loss := total_loss / loss_hand


    // 最大盈利
    max_profit, err := MaxYingLi()
    if err != nil {
        return
    }

    // 最大亏损
    max_loss, err := MaxKuiSun()
    if err != nil {
        return
    }

    // 总交易bar总数
    total_bar, err := GetTotalBar()
    if err != nil {
        return
    }
    // 总盈利交易bar总数
    // 总亏损交易bar总数
    // 总持平交易bar总数
    // 交易次数
    // 盈利次数
    // 亏损次数
    // 持平次数

    // 最大使用资金
    // 佣金合计
    // 初始资金
    // 收益率


    // 存mysql


    stat := new(Stats)
    stat.TotalProfit = total_profit
    stat.TotalLoss = total_loss
    stat.ProfitHand = profit_hand
    stat.LossHand = loss_hand
    stat.RetainedProfit = retained_profit
    stat.TotalHand = total_hand
    stat.StayHand = stay_hand
    stat.AvgRetained = avg_retained
    stat.AvgProfit = avg_profit
    stat.AvgLoss = avg_loss
    stat.MaxProfit = max_profit
    stat.MaxLoss = max_loss
    stat.TotalBar = total_bar

    fmt.Println("=========")
    fmt.Println(id)
    fmt.Println("=========")

    testHas := &Stats{Id:id}
    has , err := Engine.Get(testHas)
    if err != nil {
        return
    }
    
    fmt.Println(has, testHas)

    if has {
        fmt.Println("update======")
        _, err = Engine.Id(id).Update(stat)
    } else {
        fmt.Println("insert======")
        stat.Id = id
        _, err = Engine.Insert(stat)
    }

    if err != nil{
        return
    }

    fmt.Println("保存成功")
    return 

    /*
    var sql string

  
    if has {
        sql = "UPDATE `stats` SET `total_profit`=?,`total_loss`=?,`profit_hand`=?,`loss_hand`=?,`retained_profit`=?,`total_hand`=?,`stay_hand`=?,`avg_retained`=?,`avg_profit`=?,`avg_loss`=?,`max_profit`=?,`max_loss`=?"
    } else {
        sql = "INSERT INTO `stats`(`id`, `total_profit`,`total_loss`,`profit_hand`,`loss_hand`,`retained_profit`,`total_hand`,`stay_hand`,`avg_retained`,`avg_profit`,`avg_loss`,`max_profit`,`max_loss`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    }

    _, err = Engine.Exec(sql, )
    if err != nil {
        return 
    }
    */

    // 存redis
}


// 总交易bar总数
func GetTotalBar()(totalbar float64, err error){
    sql := "SELECT SUM(bar_num) as totalbar FROM `tb_record` WHERE is_profit > 0"
    res, err := Engine.Query(sql)
    if err != nil {
        return 
    }

    if len(res) == 0 {
        return 
    }


    f := string(res[0]["totalbar"])
    if f == "" {
        return
    }

    totalbar, err = strconv.ParseFloat(f, 64)
    return
}

// 净利润
func JingLiRun(yingli, kuisun float64) (liyun float64) {
   liyun =  yingli - kuisun
   return
}

// 总盈利, 盈利手数@todo 未算手续费
func ZongYingLi() (total, count float64, err error) {
    total = 0.00
    sql := "select sum(profit) as total, sum(number) as cnt from tb_record where is_profit = 3"
    res, err := Engine.Query(sql)
    if err != nil {
        return 
    }

    if len(res) == 0 {
        return 
    }

    cntStr := string(res[0]["cnt"])
    totalStr := string(res[0]["total"])

    if cntStr == "" {
        cntStr = "0"
    }

    if totalStr == "" {
        totalStr = "0"
    }


    count, err = strconv.ParseFloat(cntStr, 64)
    total, err = strconv.ParseFloat(totalStr, 64)

    return
}


// 总亏损, 亏损手数 @todo 未算手续费
func ZongKuiSun() (total, count float64, err error) {
    sql := "select sum(profit) as total, sum(number) as cnt from tb_record where is_profit = 1"
    res, err := Engine.Query(sql)
    if err != nil {
        return 
    }

    if len(res) == 0 {
        return 
    }


    cntStr := string(res[0]["cnt"])
    totalStr := string(res[0]["total"])

    if cntStr == "" {
        cntStr = "0"
    }

    if totalStr == "" {
        totalStr = "0"
    }

    count, err = strconv.ParseFloat(cntStr, 64)
    total, err = strconv.ParseFloat(totalStr, 64)
    total = math.Abs(total)

    return
}

//持平手数
func ZhiPingShouShu() (count float64, err error) {
    sql := "select sum(number) as cnt from tb_record where is_profit = 2"
    res, err := Engine.Query(sql)
    if err != nil {
        return 
    }

    if len(res) == 0 {
        return 
    }

    count, err = strconv.ParseFloat(string(res[0]["cnt"]), 64)

    return
}

// 总交易手数
func ZongShouShu() (count float64, err error){
    sql := "select sum(number) as cnt from tb_record where is_profit > 0"
    res, err := Engine.Query(sql)
    if err != nil {
        return 
    }

    if len(res) == 0 {
        return 
    }

    if string(res[0]["cnt"]) == "" {
        return 
    }

    count, err = strconv.ParseFloat(string(res[0]["cnt"]), 64)

    return
}

// 最大盈利
func MaxYingLi() (profit float64, err error){
    sql := "select profit from tb_record where is_profit = 3 order by profit desc limit 3"
    res, err := Engine.Query(sql)
    if err != nil {
        return 
    }

    if len(res) == 0 {
        return 
    }

    profit, err = strconv.ParseFloat(string(res[0]["profit"]), 64)

    return 
}

// 最大亏损
func MaxKuiSun() (profit float64, err error){
    sql := "select profit from tb_record where is_profit = 1 order by profit asc limit 1"
    res, err := Engine.Query(sql)
    if err != nil {
        return 
    }
    
    if len(res) == 0 {
        return 
    }

    profit, err = strconv.ParseFloat(string(res[0]["profit"]), 64)

    return 
}

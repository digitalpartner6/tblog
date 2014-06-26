package models

import(
    "fmt"
    "strconv"
    "math"
    "time"
    "errors"
    "github.com/garyburd/redigo/redis"
    )

type Finfo struct {
    Id          int64     // ID
    FormulaName string    // 名称
    Symbol      string    // 交易品种
    Capital     float64   // 本金
    Remaining   float64   // 余额
    StartDate   time.Time // 开始时间
    LastDate    time.Time // 最新交易时间
    JingZhi     float64   // 净值
    JingLiRun   float64   // 净利润
    SumYingLi   float64   // 总盈利
    MaxYingLi   float64   // 最大盈利
    SumKuiSun   float64   // 总亏损
    MaxKuiSun   float64   // 最大亏损
    CountSellTimes    int64   // 交易次数
    CountYingLiTimes  int64   // 盈利次数
    CountKuiSunTimes  int64   // 亏损次数
    RateShengLv float64   // 胜率
    AvgChiCangBar     int64   // 平均持仓时间
    CountSellDay      int64   // 总交易天数
    AvgMonthShouYi    float64 // 月平均收益
    CountYingLiNumber int64   // 盈利手数
    CountKuiSunNumber int64   // 亏损手数
    SumNumber   int64     // 总交易手数
    AvgYingLi   float64   // 平均盈利
    AvgKuiSun   float64   // 平均亏损
    RateYingKui float64   // 盈亏比
    CounterYingLi     int64   // 盈利计数
    CounterKuiSun     int64   // 亏损计数
    MaxYingLiTimes    int64   //  最大连续盈利次数
    MaxKuiSunTimes    int64   // 最大连续亏损次数
    CountSellMonths   int64   // 交易月数
    RateShouYi        float64   // 收益率
    RateMonthShouYi   float64 // 月平均收益率
    RateYearShouYi    float64 // 年化收益率
    MaxJingLiRun      float64 // 最大净利润
    MaxHuiChePrice    float64 // 最大回撤
    RateMaxHuiChe     float64 // 最大回撤百分比
    RateYearShouYiMaxHuiChe   float64 // 年化收益率/最大回撤百分比
    UpdateTime  time.Time   // 记录更新时间
}

func Save2Redis(fname, symbol string) (err error){

    /*
    info := new(Finfo)
    a, err := Engine.Where("formula_name=? and symbol=?", fname, symbol).Get(info)
    if !a {
        return errors.New("记录不存在或查询失败:"+err.Error())
    }
    */

    conn, err := redis.Dial("tcp", "192.168.0.80:6379")
    if err != nil {
        return 
    }

    defer conn.Close()

    fid, err := redis.String(conn.Do("GET", fmt.Sprintf("futures.strategy.code.to.id.%s%s", fname,symbol)))
    if err != nil {
        return
    }

    if fid == "" {
        return
    }

    key := fmt.Sprintf("futures:%s",fid)
    args := []interface{}{key}

    sql := fmt.Sprintf("SELECT * FROM `finfo` WHERE formula_name=\"%s\" AND symbol=\"%s\" limit 1", fname, symbol)
    res, err := Engine.Query(sql)
    if err != nil {
        return
    }

    if len(res) == 0 {
        return errors.New("mysql记录不存在")
    }

    for k,v := range res[0] {
        if k == "id"{
            continue
        }
        args = append(args, k, string(v))
    }

    _, err = conn.Do("HMSET", args...)
    if err != nil {
        return
    }

    fmt.Println("Save2Redis ok!")

    return
}

// 更新info信息
func DoUpdateInfo(fname, symbol string) (err error){
    yingliInfo, err := YingliInfo(fname, symbol)
    if err != nil {
        return 
    }

    // 总盈利
    sum_ying_li, err := strconv.ParseFloat(yingliInfo["sum_ying_li"], 64)
    if err != nil {
        return
    }

    // 最大盈利
    max_ying_li, err := strconv.ParseFloat(yingliInfo["max_ying_li"], 64)
    if err != nil {
        return
    }
    
    // 盈利次数
    count_ying_li_times, err := strconv.ParseFloat(yingliInfo["count_ying_li_times"], 64)
    if err != nil {
        return
    }

    // 盈利手数
    count_ying_li_number, err := strconv.ParseFloat(yingliInfo["count_ying_li_number"], 64)
    if err != nil {
        return
    }

    // 平均盈利
    avg_ying_li := 0.0
    if count_ying_li_number > 0 {
        avg_ying_li = sum_ying_li / count_ying_li_number
    }

    kuisunInfo, err := KuiSunInfo(fname, symbol)
    if err != nil {
        return
    }

    // 总亏损
    sum_kui_sun, err := strconv.ParseFloat(kuisunInfo["sum_kui_sun"], 64)
    if err != nil {
        return
    }

    // 最大亏损
    max_kui_sun, err := strconv.ParseFloat(kuisunInfo["max_kui_sun"], 64)
    if err != nil {
        return
    }

    // 亏损手数
    count_kui_sun_number, err := strconv.ParseFloat(kuisunInfo["count_kui_sun_number"], 64)
    if err != nil {
        return
    }

    // 亏损交易次数
    count_kui_sun_times, err := strconv.ParseFloat(kuisunInfo["count_kui_sun_times"], 64)
    if err != nil {
        return
    }
    // 平均亏损
    avg_kui_sun := 0.0
    if count_kui_sun_number > 0 {
        avg_kui_sun = sum_kui_sun / count_kui_sun_number
    }


    // 盈亏比
    
    rate_ying_kui := 0.0
    if avg_kui_sun != 0 {
        rate_ying_kui = math.Abs(avg_ying_li/avg_kui_sun)
    }

    // 净利润
    jing_li_run := sum_ying_li + sum_kui_sun

    baseInfo, err := BaseInfo(fname, symbol)
    if err != nil {
        return
    }

    // 本金
    capital, err := strconv.ParseFloat(baseInfo["capital"], 64)
    if err != nil {
        return
    }

    // 最大净利润
    max_jing_li_run, err := strconv.ParseFloat(baseInfo["max_jing_li_run"], 64)
    if err != nil {
        return
    }
    // 取最大净利润
    
    max_jing_li_run = math.Max(max_jing_li_run, jing_li_run)
    if max_jing_li_run == 0 {
        max_jing_li_run = jing_li_run
    }

    max_hui_che_price, err := strconv.ParseFloat(baseInfo["max_hui_che_price"], 64)
    if err != nil {
        return
    }
    // 最大回撤金额
    //max_hui_che_price = math.Max(max_hui_che_price, (max_jing_li_run - jing_li_run))
    

    // 净值
    jing_zhi := 0.0
    if capital != 0 {
        jing_zhi = (jing_li_run + capital)/capital
    }

    sumInfo, err := SumInfo(fname, symbol)
    if err != nil {
        return
    }
    
    // 总交易次数
    count_sell_times, err := strconv.ParseFloat(sumInfo["count_sell_times"], 64)
    if err != nil {
        return
    }

    // 总手数
    sum_number, err := strconv.ParseFloat(sumInfo["sum_number"], 64)
    if err != nil {
        return
    }

    // 胜率
    rate_sheng_lv := 0.0
    if count_sell_times > 0 {
      rate_sheng_lv = count_ying_li_times / count_sell_times * 100
    }


    // 收益率
    rate_shou_yi := 0.0
    if capital != 0 {
        rate_shou_yi = jing_li_run / capital * 100
    }

    // 余额
    remaining := capital + jing_li_run

  
    oldInfo := new(Finfo)
    has, err := Engine.Where("formula_name=? and symbol=?", fname, symbol).Get(oldInfo)
    if err != nil || !has{
        return
    }


    // 交易天数
    duration := oldInfo.LastDate.Sub(oldInfo.StartDate)
    count_sell_day := math.Ceil(duration.Hours()/24)
    if count_sell_day == 0 {
        count_sell_day = 1
    }
    // 交易月数 进一，不足一月数一月？
    count_sell_months := math.Ceil(count_sell_day / 30.5)

    fmt.Println("count_sell_day, ==count_sell_months==========",rate_shou_yi)

    // 月平均收益率
    rate_month_shou_yi := jing_li_run/count_sell_months / capital*100
    // 月平均收益
    avg_month_shou_yi := jing_li_run / count_sell_day * 3.05
    // 年化收益率
    rate_year_shou_yi := math.Pow(math.Ceil(count_sell_day/365), 1/rate_shou_yi)
    if rate_shou_yi < 0 {
        rate_year_shou_yi = -rate_year_shou_yi
    }

    /*
    // 最大回撤百分比
    rate_max_hui_che := 0.0
    if max_jing_li_run != 0 {
        rate_max_hui_che = (max_jing_li_run - jing_li_run) / (max_jing_li_run+capital) *100
    }
    */

   rate_year_shou_yi_max_hui_che := 0.0
    if oldInfo.RateMaxHuiChe != 0 {
        rate_year_shou_yi_max_hui_che = rate_year_shou_yi / oldInfo.RateMaxHuiChe *100
    }

    finfo := new(Finfo)
    finfo.Remaining = remaining
    finfo.JingLiRun = jing_li_run
    finfo.SumYingLi = sum_ying_li
    finfo.MaxYingLi = max_ying_li
    finfo.SumKuiSun = sum_kui_sun
    finfo.MaxKuiSun = max_kui_sun
    finfo.CountSellTimes = int64(count_sell_times)
    finfo.CountYingLiTimes = int64(count_ying_li_times)
    finfo.CountKuiSunTimes = int64(count_kui_sun_times)
    finfo.RateShengLv = rate_sheng_lv
    finfo.CountYingLiNumber = int64(count_ying_li_number)
    finfo.CountKuiSunNumber = int64(count_kui_sun_number)
    finfo.SumNumber = int64(sum_number)
    finfo.AvgYingLi = avg_ying_li
    finfo.AvgKuiSun = avg_kui_sun
    finfo.RateYingKui = rate_ying_kui
    finfo.RateShouYi = rate_shou_yi
    finfo.MaxJingLiRun = max_jing_li_run
    finfo.MaxHuiChePrice = max_hui_che_price
    finfo.JingZhi = jing_zhi
    finfo.CountSellDay = int64(count_sell_day)
    finfo.AvgMonthShouYi = avg_month_shou_yi
    finfo.RateYearShouYi = rate_year_shou_yi
    finfo.CountSellMonths = int64(count_sell_months)
    finfo.RateMonthShouYi = rate_month_shou_yi
    finfo.RateYearShouYiMaxHuiChe = rate_year_shou_yi_max_hui_che

    _, err = Engine.Where("formula_name=? and symbol=?", fname, symbol).Update(finfo)
    if err != nil {
        return
    }

    fmt.Println("===== 更新finfo成功  ====")

    /*

    sql :=fmt.Sprintf("UPDATE `info` SET `remaining`=%.6f,`jing_li_run`=%.6f,`sum_ying_li`=%.6f,`max_ying_li`=%.6f,`sum_kui_sun`=%.6f,`max_kui_sun`=%.6f,`count_sell_times`=%f,`count_ying_li_times`=%f,`count_kui_sun_times`=%f,`rate_sheng_lv`=%.2f,`avg_chi_cang_bar`=%f,`count_sell_day`=%f,`avg_month_shou_yi`=%.6f,`count_ying_li_number`=%f,`count_kui_sun_number`=%f,`sum_number`=%f,`avg_ying_li`=%.6f,`avg_kui_sun`=%.6f,`rate_ying_kui`=%.2f,`counter_ying_li`=%f,`counter_kui_sun`=%f,`max_ying_li_times`=%f,`max_kui_sun_times`=%f,`count_sell_months`=%f,`rate_shou_yi`=%.2f,`rate_month_shou_yi`=%.2f,`rate_year_shou_yi`=%.2f,`max_jing_li_run`=%.6f,`max_hui_che_price`=%.6f,`rate_max_hui_che`=%.2f,`rate_year_shou_yi_max_hui_che`=%.2f, `jing_zhi`=%.6f WHERE `formula_name`=\"%s\" and `symbol`=\"%s\"", remaining, jing_li_run, sum_ying_li, max_ying_li, math.Abs(sum_kui_sun), math.Abs(max_kui_sun), count_sell_times, count_ying_li_times, count_kui_sun_times, rate_sheng_lv, 0.0, 0.0, 0.0, count_ying_li_number, count_kui_sun_number, sum_number, avg_ying_li, avg_kui_sun, rate_ying_kui, 0.0, 0.0, 0.0, 0.0, 0.0, rate_shou_yi, 0.0, 0.0, max_jing_li_run, max_hui_che_price, 0.0, 0.0, jing_zhi, fname, symbol)

    _, err = Engine.Exec(sql)
    if err != nil {
        return
    }

    */
    return
}

/**
 * 盈利信息
 * 总盈利 sum_ying_li
 * 最大盈利 max_ying_li
 * 盈利次数 count_yin_li_times
 * 盈利手数 count_yin_li_number
 */
func YingliInfo(fname, symbol string) (list map[string]string, err error){
    sql := fmt.Sprintf("SELECT sum(profit) as sum_ying_li, max(profit) as max_ying_li, sum(number) as count_ying_li_number, count(id) as count_ying_li_times FROM `tb_record` WHERE `formula_name`='%s' and `symbol`='%s' and `is_profit`=3", fname, symbol)
    res, err := Engine.Query(sql)
    if err != nil {
        fmt.Println(err)
        return
    }

    list = make(map[string]string)
    list["sum_ying_li"] = "0"
    list["max_ying_li"] = "0"
    list["count_ying_li_number"] = "0"
    list["count_ying_li_times"] = "0"

    for k,v := range res[0] {
        list[k] = string(v)
    }

    fmt.Println(list)

    return 
}

 /**
 * 盈利信息
 * 总盈利 sum_ying_li
 * 最大盈利 max_ying_li
 * 盈利次数 count_yin_li_times
 * 盈利手数 count_yin_li_number
 */
func KuiSunInfo(fname, symbol string) (list map[string]string, err error){
    sql := fmt.Sprintf("SELECT sum(profit) as sum_kui_sun, min(profit) as max_kui_sun, sum(number) as count_kui_sun_number, count(id) as count_kui_sun_times FROM `tb_record` WHERE `formula_name`='%s' and `symbol`='%s' and `is_profit`=1", fname, symbol)
    res, err := Engine.Query(sql)
    if err != nil {
        fmt.Println(err)
        return
    }

    list = make(map[string]string)
    list["sum_kui_sun"] = "0"
    list["max_kui_sun"] = "0"
    list["count_kui_sun_number"] = "0"
    list["count_kui_sun_times"] = "0"

    for k,v := range res[0] {
        list[k] = string(v)
    }

    fmt.Println(list)

    return 
}

/**
 *  总信息
 */
func SumInfo(fname, symbol string) (list map[string]string, err error){
    sql := fmt.Sprintf("SELECT sum(number) as sum_number, count(id) as count_sell_times FROM `tb_record` WHERE `formula_name`='%s' and `symbol`='%s' and `is_profit`=1 or `is_profit`=3", fname, symbol)
     res, err := Engine.Query(sql)
    if err != nil {
        fmt.Println(err)
        return
    }

    list = make(map[string]string)
    list["sum_number"] = "0"
    list["count_sell_times"] = "0"

    for k,v := range res[0] {
        list[k] = string(v)
    }

    fmt.Println(list)

    return 
}

/**
 *  基本信息
    本金
    资金余额
    开始日期

 */
func BaseInfo(fname, symbol string) (list map[string]string, err error){
    sql := fmt.Sprintf("SELECT `capital`,`remaining`, `start_date`, `max_jing_li_run`, `max_hui_che_price` FROM `finfo` WHERE  `formula_name`='%s' and `symbol`='%s'", fname, symbol)

    fmt.Println("=== sql ===",sql)
    res, err := Engine.Query(sql)
    if err != nil {
        return
    }

    if len(res) == 0 {
        return nil, errors.New("数据不存在")
    }

    list = make(map[string]string)
    for k,v := range res[0] {
        list[k] = string(v)
    }

    fmt.Println(list)

    return
}

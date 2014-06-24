package models

import(
    "time"
    "errors"
    "fmt"
    "math"
    "strconv"
//    "fsnotice/util"
)

type TbRecord struct{
    Id string
    FormulaName string
    Symbol string
    Date string
    Time string
    Action string
    Number int32
    Price float64
    EntryPrice float64
    NowPosition int32
    Profit float64
    BarNum int32
    IsProfit int
}

func SaveTbRecord(info map[string]string) (err error){
    //tb_record

    v, ok := info["FormulaName"]
    if !ok {
        return errors.New("[ERROR]field name FormulaName no exists!")
    }
    formula_name := v

    v, ok = info["Symbol"]
    if !ok {
        return errors.New("[ERROR]field name Symbol no exists!")
    }
    symbol := v

    v, ok = info["date"]
    if !ok {
        return errors.New("[ERROR]field name date no exists!")
    }
    dateTime, err := time.Parse("20060102", v)
    if err != nil {
        return errors.New("[ERROR]field name date parse failed !")
    }
    dateStr := dateTime.Format("2006-01-02")

     v, ok = info["time"]
    if !ok {
        return errors.New("[ERROR]field name time no exists!")
    }
    f64, err := strconv.ParseFloat(v, 64);
    if err != nil {
        return errors.New("{ERROR failed to parsefloat time}")
    }
    timeTime, err := time.Parse("0.150405", fmt.Sprintf("%0.6f", f64))
    if err != nil {
        return errors.New("[ERROR]field name time parse failed !")
    }
    timeStr := timeTime.Format("15:04:05")

    v, ok = info["action"]
    if !ok {
        return errors.New("[ERROR]field name action no exists!")
    }
    action := v

    v, ok = info["number"]
    if !ok {
        return errors.New("[ERROR]field name number no exists!")
    }
    number, _ := strconv.Atoi(v)

    v, ok = info["price"]
    if !ok {
        return errors.New("[ERROR]field name price no exists!")
    }
    price,_ := strconv.ParseFloat(v, 64)

    v, ok = info["EntryPrice"]
    if !ok {
        return errors.New("[ERROR]field named EntryPrice no exists!")
    }
    entry_price,_ := strconv.ParseFloat(v, 64)

    v, ok = info["nowPosition"]
    if !ok {
        return errors.New("[ERROR]field named nowPosition no exists!")
    }
    now_position, _ := strconv.Atoi(v)

    v, ok = info["BarNum"]
    if !ok {
        return errors.New("[ERROR]field named BarNum no exists!")
    }
    bar_num, err := strconv.ParseFloat(v, 64)

    id := fmt.Sprintf("%s_%s_%s_%s_%s", formula_name, symbol, dateStr, timeStr, action)

    ex := &TbRecord{Id:id}
    has , err := Engine.Get(ex)
    if has {
        return errors.New("record exists !!") 
    }

    profit := 0.00

    // 算出利润
    if action == "sell" {
        profit =  (price - entry_price) * float64(number)
    } else if action == "buytocover" {
        profit = (entry_price - price) * float64(number)
    }


    var isProfit int = 0
    if profit > 0 {
        isProfit = 3
    } else if profit < 0 {
        isProfit = 1
    } else {
        isProfit = 2
    }

//    del := "DELETE FROM `tb_record` WHERE `formula_name`=?, `symbol`=?, `date`=?, `time`=?, `action`=?, `number`=?, `price`=?, `entry_price`=?, `now_position`=?"
//    Engine.Exec(del, args["formula_name"], args["symbol"], args["date"], args["time"], args["action"], args["number"], args["price"], args["entry_price"], args["now_position"])


    sql := "REPLACE INTO `tb_record`(`id`, `formula_name`, `symbol`, `date`, `time`, `action`, `number`, `price`, `entry_price`, `now_position`, `profit`, `is_profit`, `bar_num`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    _, err = Engine.Exec(sql, id, formula_name, symbol, dateStr, timeStr, action, number, price, entry_price, now_position, profit, isProfit, bar_num)

    if err != nil {
        return
    }

    if isProfit == 2 {
        return 
    }

    // 查出info
    fmt.Println("--- update info -------------")

    fInfo := new(Finfo)

    has, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Get(fInfo)
    if err != nil || !has {
        return
    }


    updateInfo := new(Finfo)

    updateInfo.LastDate = fInfo.LastDate
    updateInfo.CounterKuiSun = fInfo.CounterKuiSun
    updateInfo.CounterYingLi = fInfo.CounterYingLi
    updateInfo.MaxKuiSunTimes = fInfo.MaxKuiSunTimes
    updateInfo.MaxYingLiTimes = fInfo.MaxYingLiTimes

    // 最后交易 时间
    lastDate := fInfo.LastDate
    insertDate, err := time.Parse("2006-01-02", dateStr)
    if err != nil {
        return
    }

    fmt.Println(" daddadaa", insertDate)
    // 如果大于最新时间则更新
    if insertDate.After(lastDate) {
        updateInfo.LastDate = insertDate
    }

    // 最大连续盈/亏
    if isProfit == 1 {
        updateInfo.CounterKuiSun = fInfo.CounterKuiSun + 1
        updateInfo.CounterYingLi = 0
        
        if updateInfo.CounterKuiSun > fInfo.MaxKuiSunTimes {
            updateInfo.MaxKuiSunTimes = updateInfo.CounterKuiSun
        }

    } else if isProfit == 3 {
        updateInfo.CounterKuiSun = 0
        updateInfo.CounterYingLi = fInfo.CounterYingLi + 1
        if updateInfo.CounterYingLi > fInfo.MaxYingLiTimes {
            updateInfo.MaxYingLiTimes = updateInfo.CounterYingLi
        }
    }

    // 最大回撤百分比
    max_jing_li_run := math.Max((fInfo.MaxJingLiRun + profit), fInfo.MaxJingLiRun)
    if max_jing_li_run == 0 {
        updateInfo.MaxJingLiRun = fInfo.MaxJingLiRun
    } else {
        updateInfo.MaxJingLiRun = max_jing_li_run
    }

   
    _, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Cols("last_date, counter_ying_li, counter_kui_sun, max_ying_li_times, max_kui_sun_times, max_jing_li_run").Update(updateInfo)
    if err != nil {
        return
    }

    return 
}

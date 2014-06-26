package models

import(
    "time"
    "errors"
    "fmt"
    "math"
    "strconv"
//    "fsnotice/util"
    "github.com/garyburd/redigo/redis"
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


// 取策略id
func GetFuturesId(fname, symbol string) (fid string, err error){
    conn, err := redis.Dial("tcp", "192.168.0.80:6379")
    if err != nil {
        fmt.Println("no way")
        return
    }

    fid, err = redis.String(conn.Do("GET", fmt.Sprintf("futures.strategy.code.to.id.%s%s", fname,symbol)))
    if err != nil {
        return
    }

    return
}

func GetFeedId(t string) (rid string, err error) {
    conn, err := redis.Dial("tcp", "192.168.0.80:6379")
    if err != nil {
        fmt.Println("no way")
        return
    }

    defer conn.Close()

    // 取id
    ri, err := redis.Int64(conn.Do("INCR", "feed:counter"))
    //ri, err := conn.Do("INCR", "feed:counter")
    if err != nil {
        return
    }


    rid = strconv.FormatInt(ri, 10)

    if rid == "" {
        err = errors.New("数据读取失败")
        return
    }

    return rid+t, nil
}

func Record2Redis(recordId, fname, symbol string) (err error){

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
        return errors.New("GetFuturesId empty")
    }


    // 是否已存在
    siskey := fmt.Sprintf("futures:%s:all.record", fid)
    sis, err := redis.Bool(conn.Do("SISMEMBER", siskey, recordId))
    if err != nil {
        return
    }

    if sis {
        return errors.New("record exists !")
    }


    fmt.Println("Record2Redis start ..... .. ")


    sql := fmt.Sprintf("select * from tb_record where id=\"%s\"", recordId)
    res, err := Engine.Query(sql)
    if err != nil {
        return
    }

    if len(res) == 0 {
        return errors.New("record info id empty")
    }

    rid, err := GetFeedId("46")
    if err != nil {
        return
    }
    // 存hash
    key := fmt.Sprintf("futures.strategy.result:%s", rid)

    info := make(map[string]string, 0)
    args := []interface{}{key}
    for k,v := range res[0] {
        info[k] = string(v)
        if k == "id" {
            continue
        }

        args = append(args, k, string(v))
    }

    fmt.Println("save hash")


    _, err = conn.Do("HMSET", args...)
    if err != nil {
        return
    }

    // 存列表
    utime,err := time.Parse("2006-01-02 15:04:05", fmt.Sprintf("%s %s", info["date"], info["time"]))
    if err != nil {
        return
    }

    // daily. futures.strategy.result.by.result.id:[ futures.stragegy.id]:all
    _, err = conn.Do("ZADD", fmt.Sprintf("daily.futures.strategy.result.by.result.id:%s:all", fid), strconv.FormatInt(utime.Unix(), 10), rid)
    if err != nil {
        return
    }

    // daily. futures.strategy.result.by.result.id:[ futures.stragegy.id]:[yyyy-mm-dd]
    _, err = conn.Do("ZADD", fmt.Sprintf("daily.futures.strategy.result.by.result.id:%s:%s", fid, utime.Format("2006-01-02")),strconv.FormatInt(utime.Unix(), 10), rid)
    if err != nil {
        return
    }

    // 存到已存在set
    _, err = conn.Do("SADD", siskey, recordId)
    if err != nil {
        return
    }


    fmt.Println("存record to redis ok")
    return
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

    fmt.Println("begin Record2Redis")

    // 存到reids
    err = Record2Redis(id, formula_name, symbol)
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

    // 净利润
    updateInfo.JingLiRun = fInfo.JingLiRun + profit

    // 最大净利润
    max_jing_li_run := math.Max(updateInfo.JingLiRun, fInfo.MaxJingLiRun)
    if max_jing_li_run == 0 {
        updateInfo.MaxJingLiRun = fInfo.MaxJingLiRun
    } else {
        updateInfo.MaxJingLiRun = max_jing_li_run
    }

    // 最大回撤金额
    max_hui_che := max_jing_li_run - updateInfo.JingLiRun
    updateInfo.MaxHuiChePrice = math.Max(fInfo.MaxHuiChePrice, max_hui_che)

    // 交易后的余额，存入每天余额列表
    updateInfo.Remaining = fInfo.Remaining + profit

    // 最大回撤百分比
    if isProfit == 1 {
        updateInfo.RateMaxHuiChe = (updateInfo.MaxJingLiRun - updateInfo.JingLiRun) / (updateInfo.MaxJingLiRun + fInfo.Capital) * 100
        _, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Cols("last_date, counter_ying_li, counter_kui_sun, max_ying_li_times, max_kui_sun_times, max_jing_li_run, remaining, max_hui_che_price, jing_li_run, RateMaxHuiChe").Update(updateInfo)
    } else {
   
        _, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Cols("last_date, counter_ying_li, counter_kui_sun, max_ying_li_times, max_kui_sun_times, max_jing_li_run, remaining, max_hui_che_price, jing_li_run").Update(updateInfo)
    }
    if err != nil {
        return
    }

    // 保存日期记录
    SaveDaliyData(formula_name, symbol, insertDate, updateInfo.Remaining)

    return 
}

// 保存日期记录
func SaveDaliyData(formula_name, symbol string, date time.Time, remaining float64) (err error){
    conn, err := redis.Dial("tcp", "192.168.0.80:6379")
    if err != nil {
        return 
    }

    defer conn.Close()

    fid, err := GetFuturesId(formula_name, symbol)
    if err != nil {
        return
    }

    _, err = conn.Do("ZADD", fmt.Sprintf("futures:%s:daily.data", fid), remaining, date.Unix())
    if err != nil {
        return
    }

    return
}

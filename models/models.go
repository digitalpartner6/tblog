package models

import(
    "fmt"
    "time"
    "errors"
    "strconv"
    "math"
    "github.com/Unknwon/goconfig"
    "github.com/weisd/tblog/conf"
   _ "github.com/go-sql-driver/mysql"
   "github.com/go-xorm/xorm"
    "github.com/garyburd/redigo/redis"
)

var (
    Cfg *goconfig.ConfigFile
    Engine *xorm.Engine
    RedisPool *redis.Pool
)

func init(){
    var err error
    Cfg, err = conf.NewCfg("./conf.ini")
    checkErr(err)

    user := Cfg.MustValue("mysql", "user")
    pass := Cfg.MustValue("mysql", "passwd")
    host := Cfg.MustValue("mysql", "host")
    port := Cfg.MustValue("mysql", "port")
    dbname := Cfg.MustValue("mysql", "dbname")
    charset := Cfg.MustValue("mysql", "charset")

    Engine, err = NewXorm(user, pass, dbname, host, port, charset)
    checkErr(err)


    server  := Cfg.MustValue("redis", "server")
    pwd  := Cfg.MustValue("redis", "password")

    RedisPool = NewRedis(server, pwd)
}

// mysql engine
func NewXorm(user, pass, dbname, host, port, charset string)(eg *xorm.Engine, err error){
    dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", user, pass, host, port, dbname, charset)
    eg, err = xorm.NewEngine("mysql", dns)
    if err != nil {
        return
    }

    return
}

// redis 连接池
func NewRedis(server, pass string) *redis.Pool {
    return &redis.Pool{
        MaxIdle:3,
        IdleTimeout:240*time.Second,
        Dial:func()(redis.Conn, error){
            c, err := redis.Dial("tcp", server)
            if err != nil {
                return nil, err
            }

            if pass != "" {
	            if _, err := c.Do("AUTH", pass); err != nil {
	                c.Close()
	                return nil, err
	            }
            }

            return c,nil
        },
        TestOnBorrow:func(c redis.Conn, t time.Time) error{
            _, err := c.Do("PING")
            return err
        },
    }
}

// 错误退出 
func checkErr(err error){
    if err != nil {
        panic(err)
    }
}

// 交易记录
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

// 保存交易记录到mysql
func SaveTbRecord(info map[string]string) (err error){
    //tb_record

    formula_name, ok := info["FormulaName"]
    if !ok {
        return errors.New("[ERROR]field name FormulaName no exists!")
    }

    symbol, ok := info["Symbol"]
    if !ok {
        return errors.New("[ERROR]field name Symbol no exists!")
    }

    v, ok := info["date"]
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

    sql := "REPLACE INTO `tb_record`(`id`, `formula_name`, `symbol`, `date`, `time`, `action`, `number`, `price`, `entry_price`, `now_position`, `profit`, `is_profit`, `bar_num`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    _, err = Engine.Exec(sql, id, formula_name, symbol, dateStr, timeStr, action, number, price, entry_price, now_position, profit, isProfit, bar_num)

    if err != nil {
        return
    }

    conn := RedisPool.Get()
    defer conn.Close()

    // 存到reids
    err = Record2Redis(conn, id, formula_name, symbol)
    if err != nil {
        return
    }

    if isProfit == 2 {
        return 
    }

    // 查出info

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
        _, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Cols("last_date, counter_ying_li, counter_kui_sun, max_ying_li_times, max_kui_sun_times, max_jing_li_run, remaining, max_hui_che_price, jing_li_run, rate_max_hui_che").Update(updateInfo)
    } else {
   
        _, err = Engine.Where("formula_name=? and symbol=?", formula_name, symbol).Cols("last_date, counter_ying_li, counter_kui_sun, max_ying_li_times, max_kui_sun_times, max_jing_li_run, remaining, max_hui_che_price, jing_li_run").Update(updateInfo)
    }
    if err != nil {
        return
    }

    // 保存日期记录
    err = SaveDaliyData(conn, formula_name, symbol, insertDate, updateInfo.Remaining)
    if err != nil {
        return
    }

    return 
}

// 取策略id
func GetFuturesId(conn redis.Conn,fname, symbol string) (fid string, err error){
    fid, err = redis.String(conn.Do("GET", GetFuturesIdKey(fname, symbol)))
    if err != nil {
        return
    }

    return
}

// 取fid的key
func GetFuturesIdKey(fname, symbol string) string{
    return fmt.Sprintf("futures.strategy.code.to.id.%s%s", fname,symbol)
}

// 取feedID
func GetFeedId(conn redis.Conn, t string) (rid string, err error) {
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

//是否存在key
func GetRecordExistsKey(fid string) string {
    return fmt.Sprintf("futures:%s:all.record", fid)
}
// 是存已存在redis中
func CheckRecordExistsRedis(conn redis.Conn, fid, recordId string) (has bool, err error) {
    return redis.Bool(conn.Do("SISMEMBER", GetRecordExistsKey(fid), recordId))
}

// 取record hash key
func GetRecordInfoKey(rid string) string{
    return fmt.Sprintf("futures.strategy.result:%s", rid)
}

// 从mysql中同步记录到redis
func Record2Redis(conn redis.Conn, recordId, fname, symbol string) (err error){

    fid, err := GetFuturesId(conn, fname, symbol)
    if err != nil {
        return
    }

    if fid == "" {
        return errors.New("GetFuturesId empty")
    }


    // 是否已存在
    sis, err := CheckRecordExistsRedis(conn, fid, recordId)
    if err != nil {
        return
    }

    // 已存在退出 
    if sis {
        // @todo 已存在提示
        // return errors.New("record exists !")
        return
    }

    sql := fmt.Sprintf("select * from tb_record where id=\"%s\"", recordId)
    res, err := Engine.Query(sql)
    if err != nil {
        return
    }

    if len(res) == 0 {
        return errors.New("record info is empty")
    }

    rid, err := GetFeedId(conn, "46")
    if err != nil {
        return
    }
    // 存hash
    key := GetRecordInfoKey(rid)

    args := []interface{}{key}
    info := make(map[string]string, 0)
    for k,v := range res[0] {
        info[k] = string(v)
        if k == "id" {
            continue
        }

        args = append(args, k, string(v))
    }

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
    _, err = conn.Do("SADD", GetRecordExistsKey(fid), recordId)
    if err != nil {
        return
    }

    return
}

// 保存日期记录
func SaveDaliyData(conn redis.Conn, formula_name, symbol string, date time.Time, remaining float64) (err error){
    fid, err := GetFuturesId(conn, formula_name, symbol)
    if err != nil {
        return
    }

    _, err = conn.Do("ZADD", fmt.Sprintf("futures:%s:daily.data", fid), remaining, date.Unix())
    if err != nil {
        return
    }

    return
}

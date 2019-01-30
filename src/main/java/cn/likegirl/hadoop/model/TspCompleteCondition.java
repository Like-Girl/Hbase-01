package cn.likegirl.hadoop.model;

import cn.likegirl.hadoop.framework.annotations.RowKey;

import java.util.Date;

/**
 * 综合车况数据
 */
@RowKey(value = "rowKey")
public class TspCompleteCondition {
    private String rowKey;
    private Integer id;
    private Date acquisitionTime;//采集时间
    private Float batteryVoltage;//电池电压
    private Float engineSpeed;//发动机转速
    private Float fuel;//燃料
    private Double latitude;//纬度
    private Double longitude;//经度
    private Float totalDistance;//总里程
    private Float vehBatSOC;//电池电量
    private Float vehicleSpeed;//车速
    private String vin;
    private Date timestamp;//转发时间
    private Date createTime;

    private String plateLicenseNo;//车牌号
    private Date searchTime;//查询时间，用于在Redis中表明当日前的历史最新一条记录是哪一天更新的
    private Float temperature1;//5个温度备用
    private Float temperature2;
    private Float temperature3;
    private Float temperature4;
    private Float temperature5;
    private Integer humidity1;//湿度
    private Integer humidity2;//湿度
    private Integer humidity3;//湿度
    private Integer humidity4;//湿度
    private Integer humidity5;//湿度
    private Integer photoreceptor1;//光感
    private Integer photoreceptor2;//光感
    private Integer photoreceptor3;//光感
    private Integer photoreceptor4;//光感
    private Integer photoreceptor5;//光感
    private Date temptimestamp1;//温湿度设备时间
    private Date temptimestamp2;//温湿度设备时间
    private Date temptimestamp3;//温湿度设备时间
    private Date temptimestamp4;//温湿度设备时间
    private Date temptimestamp5;//温湿度设备时间

    /**
     * 2018-11-11添加江淮车况可获得的数据
     */
    private Float instantFuel;//瞬时油耗
    private Float averageFuel;//平均油耗
    //下面几项没有加在数据库中
    private Integer angle;//车辆行驶角度
    private Integer accStatus;//ACC状态，0是关，1是开
    private String locationMode;//定位方式，GPS/LBS/BD
    private Integer antennaStatus;//天线状态，0是正常，1是异常


    private Integer source;//综合数据的来源，1=TBOX，2=奥一，3=其他温湿度设备，4=adas

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Float getEngineSpeed() {
        return engineSpeed;
    }

    public void setEngineSpeed(Float engineSpeed) {
        this.engineSpeed = engineSpeed;
    }

    public Float getFuel() {
        return fuel;
    }

    public void setFuel(Float fuel) {
        this.fuel = fuel;
    }

    public Float getTotalDistance() {
        return totalDistance;
    }

    public void setTotalDistance(Float totalDistance) {
        this.totalDistance = totalDistance;
    }

    public Float getVehBatSOC() {
        return vehBatSOC;
    }

    public void setVehBatSOC(Float vehBatSOC) {
        this.vehBatSOC = vehBatSOC;
    }

    public Float getVehicleSpeed() {
        return vehicleSpeed;
    }

    public void setVehicleSpeed(Float vehicleSpeed) {
        this.vehicleSpeed = vehicleSpeed;
    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getAcquisitionTime() {
        return acquisitionTime;
    }

    public void setAcquisitionTime(Date acquisitionTime) {
        this.acquisitionTime = acquisitionTime;
    }

    public Float getBatteryVoltage() {
        return batteryVoltage;
    }

    public void setBatteryVoltage(Float batteryVoltage) {
        this.batteryVoltage = batteryVoltage;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Date getSearchTime() {
        return searchTime;
    }

    public void setSearchTime(Date searchTime) {
        this.searchTime = searchTime;
    }

    public String getPlateLicenseNo() {
        return plateLicenseNo;
    }

    public void setPlateLicenseNo(String plateLicenseNo) {
        this.plateLicenseNo = plateLicenseNo;
    }

    public Float getTemperature1() {
        return temperature1;
    }

    public void setTemperature1(Float temperature1) {
        this.temperature1 = temperature1;
    }

    public Float getTemperature2() {
        return temperature2;
    }

    public void setTemperature2(Float temperature2) {
        this.temperature2 = temperature2;
    }

    public Float getTemperature3() {
        return temperature3;
    }

    public void setTemperature3(Float temperature3) {
        this.temperature3 = temperature3;
    }

    public Float getTemperature4() {
        return temperature4;
    }

    public void setTemperature4(Float temperature4) {
        this.temperature4 = temperature4;
    }

    public Float getTemperature5() {
        return temperature5;
    }

    public void setTemperature5(Float temperature5) {
        this.temperature5 = temperature5;
    }

    public Integer getHumidity1() {
        return humidity1;
    }

    public void setHumidity1(Integer humidity1) {
        this.humidity1 = humidity1;
    }

    public Integer getHumidity2() {
        return humidity2;
    }

    public void setHumidity2(Integer humidity2) {
        this.humidity2 = humidity2;
    }

    public Integer getHumidity3() {
        return humidity3;
    }

    public void setHumidity3(Integer humidity3) {
        this.humidity3 = humidity3;
    }

    public Integer getHumidity4() {
        return humidity4;
    }

    public void setHumidity4(Integer humidity4) {
        this.humidity4 = humidity4;
    }

    public Integer getHumidity5() {
        return humidity5;
    }

    public void setHumidity5(Integer humidity5) {
        this.humidity5 = humidity5;
    }

    public Integer getPhotoreceptor1() {
        return photoreceptor1;
    }

    public void setPhotoreceptor1(Integer photoreceptor1) {
        this.photoreceptor1 = photoreceptor1;
    }

    public Integer getPhotoreceptor2() {
        return photoreceptor2;
    }

    public void setPhotoreceptor2(Integer photoreceptor2) {
        this.photoreceptor2 = photoreceptor2;
    }

    public Integer getPhotoreceptor3() {
        return photoreceptor3;
    }

    public void setPhotoreceptor3(Integer photoreceptor3) {
        this.photoreceptor3 = photoreceptor3;
    }

    public Integer getPhotoreceptor4() {
        return photoreceptor4;
    }

    public void setPhotoreceptor4(Integer photoreceptor4) {
        this.photoreceptor4 = photoreceptor4;
    }

    public Integer getPhotoreceptor5() {
        return photoreceptor5;
    }

    public void setPhotoreceptor5(Integer photoreceptor5) {
        this.photoreceptor5 = photoreceptor5;
    }

    public Date getTemptimestamp1() {
        return temptimestamp1;
    }

    public void setTemptimestamp1(Date temptimestamp1) {
        this.temptimestamp1 = temptimestamp1;
    }

    public Date getTemptimestamp2() {
        return temptimestamp2;
    }

    public void setTemptimestamp2(Date temptimestamp2) {
        this.temptimestamp2 = temptimestamp2;
    }

    public Date getTemptimestamp3() {
        return temptimestamp3;
    }

    public void setTemptimestamp3(Date temptimestamp3) {
        this.temptimestamp3 = temptimestamp3;
    }

    public Date getTemptimestamp4() {
        return temptimestamp4;
    }

    public void setTemptimestamp4(Date temptimestamp4) {
        this.temptimestamp4 = temptimestamp4;
    }

    public Date getTemptimestamp5() {
        return temptimestamp5;
    }

    public void setTemptimestamp5(Date temptimestamp5) {
        this.temptimestamp5 = temptimestamp5;
    }

    public Float getInstantFuel() {
        return instantFuel;
    }

    public void setInstantFuel(Float instantFuel) {
        this.instantFuel = instantFuel;
    }

    public Float getAverageFuel() {
        return averageFuel;
    }

    public void setAverageFuel(Float averageFuel) {
        this.averageFuel = averageFuel;
    }

    public Integer getAccStatus() {
        return accStatus;
    }

    public void setAccStatus(Integer accStatus) {
        this.accStatus = accStatus;
    }

    public String getLocationMode() {
        return locationMode;
    }

    public void setLocationMode(String locationMode) {
        this.locationMode = locationMode;
    }

    public Integer getAntennaStatus() {
        return antennaStatus;
    }

    public void setAntennaStatus(Integer antennaStatus) {
        this.antennaStatus = antennaStatus;
    }

    public Integer getSource() {
        return source;
    }

    public void setSource(Integer source) {
        this.source = source;
    }

    public Integer getAngle() {
        return angle;
    }

    public void setAngle(Integer angle) {
        this.angle = angle;
    }

}

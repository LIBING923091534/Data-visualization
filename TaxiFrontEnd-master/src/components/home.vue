<template>
  <div class="container">
    <div class="title">
      <img src="../assets/images/title.png"/>
    </div>
    <div class="tips-container" v-show="tips.show">
      <p>提示</p>
      <p>{{ tips.content }}</p>
    </div>
    <div class="map-body" id="map-body" ref="map">
    </div>
    <style-bar v-on:changeStyle="changeStyle"></style-bar>
    <timeline-bar v-bind:timeBarList="timeBarList" v-show="showTimelineBar" > </timeline-bar>
    <div class="toggle-control">
      <button @click="showControlBar=!showControlBar" size="mini">{{ showControlBar?'隐藏':'显示' }}</button>
    </div>
    <control-bar v-bind:selectedArea="selectedArea" v-bind:statisticSummary="statisticSummary"
                 v-bind:statisticInfo="statisticInfo"
                 v-show="showControlBar" v-on:clickSelect="clickSelect" v-on:clickSearch="clickSearch"> </control-bar>
    <input hidden type="file" id="select-file" @change="selectImportFile()" accept=".geojson"/>
    <popup-area-select v-on:closePopupAreaSelect="closePopupAreaSelect" v-show="showAreaSeletPopup"></popup-area-select>
  </div>
</template>

<script>
  import echarts from "echarts"
  import 'echarts-gl'
  import mapboxgl from 'mapbox-gl';

  import 'echarts/map/js/world'
  import 'echarts/lib/config'
  import 'echarts/extension/bmap/bmap'

  import MapboxLanguage from '@mapbox/mapbox-gl-language'
  import MapboxDraw from '@mapbox/mapbox-gl-draw'


  import ControlBar from "./ControlBar";
  import PopupAreaSelect from "./PopupAreaSelect";
  import TimelineBar from "./TimelineBar";
  import StyleBar from "./StyleBar";

  export default {
    name: "home",
    components: {
      StyleBar,
      TimelineBar,
      PopupAreaSelect,
      ControlBar
    },
    data() {
      return {
        mapboxToken: 'pk.eyJ1IjoidnY1NDU0NTQiLCJhIjoiY2o4NDZwcHY2MDV6MzMzczV5eTBtbnZybyJ9.LhlZtGKozugZK7_bWSKgOQ',
        chart: null,
        map: null,
        mapboxDraw: null,
        baseUrl: 'http://localhost:18080/taxiHttpServer/Search',
        // 最大显示数量
        maxView: 1000,
        // 选择的carID
        selectedID: "",
        // 选择的时间
        selectedTime: {start: 1391100000, end: 1391120000},
        // 区域选择的类型(不限，单选，起终点)
        selectedAreaType: 0,
        // 分别单区域查询和双区域查询（起终点）
        selectedArea: [{selected: false, coords: []}, {selected: false, coords: []}, {selected: false, coords: []}],
        // 选区时，当前选区编号（0（单选）,1,2（起终点））
        currentSelectNum: 0,
        // 总览信息
        statisticSummary: {
          pathCount: '加载中...',
          carCount: '加载中...',
          startTime: '--',
          endTime: '--'
        },
        // 统计信息
        statisticInfo: {
          count: 265, avgSpeed: '23.9', avgMile: '6.122',
          barSpeed: [12, 12, 12, 12,12,15],
          barMile: [10, 15, 30, 45, 5,61]
        },
        // 时间轴信息
        timeBarList:[],
        tips: {content: "", show: false},
        showTimelineBar: true,
        showControlBar: true,
        showAreaSeletPopup: false,
        option: {
          animation: false,
          backgroundColor: '#000',
          title: {
            text: '10000000 GPS Points',
            left: 'center',
            textStyle: {
              color: '#fff'
            }
          },
          mapbox: {
            animation: false,
            center: [114.3017578125, 30.5906370269],
            zoom: 4.2,
            // pitch: 60,  // 倾斜度
            // bearing: 0,  // 北朝向
            style: 'mapbox://styles/mapbox/dark-v9',
            boxHeight: 2,
          },
          series: [{
            animation: false,
            name: '弱',
            type: 'scatter3D',
            progressive: 1e6,
            coordinateSystem: 'mapbox',
            blendMode: 'lighter',
            itemStyle: {
              borderWidth: 0.1,
              borderColor: '#eee'
            },
            silent: true,
            // dimensions: ['lng', 'lat'],
            // data: new Float32Array()
            data: [[121.46106499999999, 30.911165999999998],
              [120.22291499999999, 30.202965999999996]]
          }]
        },
        CHUNK_COUNT: 1,
        dataCount: 0,
        optionLines3D: {
          animation: false,
          progressiveThreshold: 500,
          progressive: 200,
          backgroundColor: '#111',
          mapbox: {
            center: [114.3207578125, 30.5906370269],
            zoom: 11,
            pitch: 45,  // 倾斜度
            // bearing: 0,  // 北朝向
            style: 'mapbox://styles/mapbox/dark-v9',
            boxHeight: 2,
          },
          series: [{
            type: 'lines3D',

            coordinateSystem: 'mapbox',

            blendMode: 'lighter',

            // dimensions: ['value'],

            // data: new Float64Array(),
            // data:{
            //   count: 1,
            //   getItem:  {"coords":[114.3017578125, 30.5906370269, 118.3017578125, 36.5906370269]}
            // },
            // data: [{"coords": [[114.3017578125, 30.5906370269], [118.3017578125, 36.5906370269], [120.3017578125, 40.5906370269]]}],
            data: [{"coords": [[114.3017578125, 30.5906370269]]}],
            polyline: true,
            large: true,

            effect: {
              show: false,
              trailWidth: 2,
              trailLength: 0.1,
              trailOpacity: 1,
              constantSpeed: 0.5,
              spotIntensity:100,
              // trailColor: 'rgb(30, 30, 60)'
            },
            lineStyle: {
              color: "#1D6AB3",
              width: 0.8,
              opacity: 0.6
            },
            markLine: {
              silent: false
            },
            silent: false
          }]
        },
      }
    },
    mounted() {
      // 占满屏幕
      changeSize()
      // 初始化echarts和mapbox
      this.init()
      // 获取总览数据
      this.fetchSummaryData()
      // 获取轨迹数据
      this.fetchData(this.baseUrl, this.getParams())

    },
    methods: {
      init(params) {
        mapboxgl.accessToken = this.mapboxToken
        window.mapboxgl = mapboxgl
        this.chart = echarts.init(document.getElementById('map-body'), 'light');
        this.chart.setOption(this.optionLines3D, true);
        // this.chart.on('click', function (param) {
        //   console.log(param);
        // });
        // this.chart.on('mouseover', function (param) {
        //   console.log(param);
        // });
        window.onresize = () => {
          this.chart.resize()
        }
        this.map = this.chart.getModel().getComponent('mapbox3D').getMapbox()
        let map = this.map
        // 添加导航控件
        map.addControl(new mapboxgl.NavigationControl())
        // 设置语言
        map.addControl(new MapboxLanguage({
          defaultLanguage: 'zh'
        }))
        // 添加绘制控件
        this.mapboxDraw = new MapboxDraw({
          displayControlsDefault: false,
          controls: {
            polygon: true
          }
        });
        let draw = this.mapboxDraw
        map.addControl(draw);
      },

      fetchData(url, params) {
        // if (idx >= this.CHUNK_COUNT) {
        //   return;
        // }
        this.chart.showLoading('default',{
          text: '数据加载中...',
          color: '#ea006c',
          textColor: '#1676ff',
          maskColor: 'rgba(255, 255, 255, 0.6)',
          zlevel: 0
        });
        let dataURL = url;
        // let dataURL = "/static/Search";
        let xhr = new XMLHttpRequest();
        xhr.open('POST', dataURL, true);
        xhr.responseType = 'arraybuffer';

        let that = this
        xhr.onload = function (e) {
          that.resetStatistic()
          let rawData = new Float32Array(this.response);
          // 车速： ["<5", "5-10", "10-20", "20-30", "30-40", ">40"]
          // 里程： ["<2", "2-5", "5-8", "8-12", "12-16", ">16"]
          let speedList = [[0, 5], [5, 10], [10, 20], [20, 30], [30, 40], [40, 99999]]
          let mileList = [[0, 2], [2, 5], [5, 8], [8, 12], [12, 16], [16, 99999]]
          // 时间范围表(按所查询时间，分为8份, 9个数据)
          let timeList = []
          // 存储时间分布数据的列表
          let timeInfoArray = []

          let divideNum = parseInt((parseInt(that.selectedTime.end) - parseInt(that.selectedTime.start)) / 1800.0)
          divideNum = divideNum < 8 ? 8 : divideNum
          let timeInterval = (parseInt(that.selectedTime.end) - parseInt(that.selectedTime.start))/divideNum
          for (let i=0; i<=divideNum; i++){
            timeList.push(that.selectedTime.start+i*timeInterval)
            timeInfoArray.push(0)
          }

          let testPoints = 0

          let speedSum = 0.0
          let mileSum = 0.0
          // 数据格式：（起始时间1+终止时间1+平均速度1+里程1+轨迹点数1+1号点X+1号点Y+2号点X+2号点Y）+（起始时间2+...
          let lines = []
          let addedDataCount = 0;
          for (let i = 0; i < rawData.length;) {
            let line = {"coords": []}
            let startTime = rawData[i++];
            let endTime = rawData[i++];

            let avgSpeed = rawData[i++];
            speedSum += avgSpeed
            let afterLength = rawData[i++] / 1000.0;
            mileSum += afterLength

            // 构造轨迹数组
            let len = parseInt(rawData[i++]);
            testPoints += len
            for (let j = 0; j < len; j++) {
              let y = rawData[i++];
              let x = rawData[i++];
              line.coords.push([x, y])
            }
            // 车速统计
            for (let item in speedList) {
              if (avgSpeed > speedList[item][0] && avgSpeed <= speedList[item][1]) {
                that.statisticInfo.barSpeed[item]++
                break
              }
            }
            // 里程统计
            for (let item in mileList) {
              if (afterLength > mileList[item][0] && afterLength <= mileList[item][1]) {
                that.statisticInfo.barMile[item]++
                break
              }
            }
            // 时间分布统计
            for (let item in timeList) {
              if (timeList[item] > startTime && timeList[item] <= endTime) {
                timeInfoArray[item]++
                break
              }
            }
            lines.push(line)
          }
          that.statisticInfo.count = lines.length
          that.statisticInfo.avgSpeed = "" + (speedSum / lines.length).toFixed(2)
          that.statisticInfo.avgMile = "" + (mileSum / lines.length).toFixed(2)
          that.timeBarList = []
          for(let i =0; i<timeList.length;i++){
            that.timeBarList.push([timeList[i]*1000, timeInfoArray[i]])
          }

          that.optionLines3D.series[0].data = lines
          that.renderLines()
          // that.chart.setOption(that.optionLines3D, true);
          // that.chart.appendData({
          //   seriesIndex: 0,
          //   data: lines
          // });
          that.dataCount += addedDataCount;
          that.chart.hideLoading();
          console.log(testPoints)
        }
        xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
        xhr.send('params=' + params);
      },

      fetchSummaryData(){
        this.$http.get('/taxiHttpServer/Search').then((res)=>{
          let json = res.body
          this.statisticSummary.startTime = json.dateInfo.minDate_count.min
          this.statisticSummary.endTime = json.dateInfo.minDate_count.max
          this.statisticSummary.carCount = json.carNum
          this.statisticSummary.pathCount = json.totalNum
        })
      },

      // 手动选择确定后的回调函数
      callbackSelete() {
        let data = this.mapboxDraw.getAll()
        this.showControlBar = true
        this.showTimelineBar = true
        this.tips.show = false
        if (data.features.length > 0) {
          this.selectedArea[this.currentSelectNum].id = data.features[data.features.length - 1].id
          this.selectedArea[this.currentSelectNum].selected = true
          this.selectedArea[this.currentSelectNum].coords = data.features[data.features.length - 1].geometry.coordinates
          // console.log(JSON.stringify(data.features[data.features.length - 1]))
        } else {
        }
      },
      changeDrawStatus() {
        this.map.off('draw.create', this.callbackSelete)
        this.map.on('draw.create', this.callbackSelete)
      },
      clickSelect(num, type) {
        // 点击了手动选择
        this.currentSelectNum = num
        if (type === 'manual') {
          this.changeDrawStatus()
          $(".mapbox-gl-draw_ctrl-draw-btn.mapbox-gl-draw_polygon").click()
          this.showControlBar = false
          this.showTimelineBar = false
          this.tips.content = '请单击鼠标左键确定范围,双击最后一个顶点结束选择';
          this.tips.show = true
        }
        if (type === 'auto') {
          this.showAreaSeletPopup = true
        }
        if (type === 'file') {
          $("#select-file").click()
        }
        // 点击了删除选择
        else if (type === 'delete') {
          this.selectedArea[num].selected = false
          this.selectedArea[num].coords = []
          this.mapboxDraw.delete(this.selectedArea[num].id)
        }
      },
      // 常用区域 确定选择/关闭 回调
      closePopupAreaSelect(selectItem) {
        let feature = selectItem ? selectItem.feature : null
        this.showAreaSeletPopup = false
        if (feature) {
          this.selectedArea[this.currentSelectNum].id = feature.id
          this.selectedArea[this.currentSelectNum].selected = true
          this.selectedArea[this.currentSelectNum].coords = feature.geometry.coordinates
          this.mapboxDraw.add(feature)
        }
      },
      // 选择文件后 回调
      selectImportFile() {
        let selectedFile = document.getElementById('select-file').files[0];
        let reader = new FileReader()
        reader.readAsText(selectedFile)
        reader.onload = (file) => {
          try {
            let feature = JSON.parse(file.currentTarget.result)
            this.selectedArea[this.currentSelectNum].id = feature.id ? feature.id : Math.random().toString()
            this.selectedArea[this.currentSelectNum].selected = true
            this.selectedArea[this.currentSelectNum].coords = feature.geometry.coordinates
            this.mapboxDraw.add(feature)
          } catch (e) {
            alert("请导入正确的geojson格式文件")
          }
        }
      },
      // 点击查询后的响应函数
      clickSearch(maxView, carInfo, time, areaType) {
        this.maxView = maxView
        this.selectedID = carInfo.ID
        this.selectedTime = time
        this.selectedAreaType = areaType
        if ((areaType === 1 && !this.selectedArea[0].selected) ||
          (areaType === 2 && (!this.selectedArea[1].selected && !this.selectedArea[2].selected))) {
          return alert("区域选择有误！")
        }
        this.fetchData(this.baseUrl, this.getParams())
      },
      getParams() {
        let params = {}
        if(this.maxView >= 0){
          params["maxView"] = this.maxView
        }

        if(this.selectedID !== ""){
          params["carID"] = this.selectedID
        }

        if (this.selectedTime.start) {
          params["st"] = this.selectedTime.start + ""
        }
        if (this.selectedTime.end) {
          params["et"] = this.selectedTime.end + ""
        }
        if (this.selectedAreaType === 1 && this.selectedArea[0].selected) {
          let list = this.selectedArea[0].coords[0].map(function (v) {
            let s = v.reverse().join(",")
            v.reverse()
            return s
          })
          let str = list.join(";")
          params["area1"] = str
        }
        if (this.selectedAreaType === 2 && this.selectedArea[1].selected) {
          let list1 = this.selectedArea[1].coords[0].map(function (v) {
            let s = v.reverse().join(",")
            v.reverse()
            return s
          })
          let str1 = list1.join(";")
          params["area2"] = str1
        }
        if (this.selectedAreaType === 2 && this.selectedArea[2].selected) {
          let list2 = this.selectedArea[2].coords[0].map(function (v) {
            let s = v.reverse().join(",")
            v.reverse()
            return s
          })
          let str2 = list2.join(";")
          params["area3"] = str2
        }

        return JSON.stringify(params)
      },
      // 重置统计值
      resetStatistic(){
        this.statisticInfo.barSpeed = [0,0,0,0,0,0]
        this.statisticInfo.barMile= [0,0,0,0,0,0]
        // this.statisticInfo.timeBarList = []
      },
      changeStyle(style){
        this.optionLines3D.series[0].lineStyle.color = style.color
        this.optionLines3D.series[0].lineStyle.width = style.width
        this.optionLines3D.series[0].lineStyle.opacity = style.opacity
        this.optionLines3D.series[0].effect.show = style.effect
        this.optionLines3D.mapbox.style = style.mapStyle==='1' ? 'mapbox://styles/mapbox/dark-v9' : 'mapbox://styles/mapbox/streets-v9'
        this.renderLines()
      },
      // 开始渲染、重新渲染
      renderLines(){
        console.log(Date.now())
        this.chart.setOption(this.optionLines3D, true);
        console.log(Date.now())
      }
    },
    beforeDestroy() {
      this.chart.clear()
    },
  }

  function changeSize() {
    let domMapBody = document.getElementById("map-body");
    domMapBody.style.width = window.innerWidth + "px";
    domMapBody.style.height = window.innerHeight + "px";
  }


</script>

<style scoped>
  .container {
    text-align: center;
    width: 100%;
  }

  .title {
    position: fixed;
    top: 0;
    left: 20px;
    width: 328px;
    background: rgba(0, 0, 0, 0.5);
    z-index: 999;
  }

  .title img {
    width: 100%;
  }

  .tips-container {
    position: fixed;
    top: 30px;
    right: 80px;
    width: 280px;
    min-height: 80px;
    background: rgba(169, 169, 169, 0.62);
    z-index: 99;
    text-align: left;
    padding: 10px;
    border-radius: 5px;
    color: lightyellow;
  }

  .tips-container p {
    margin-top: 2px;
  }

  .map-body {
    width: 800px;
    height: 500px;
    margin: 0 auto;
  }

  .toggle-control{
    position: fixed;
    bottom: 1px;
    right: 1px;
    width: 45px;
    height: 38px;
    z-index: 999;
  }
  .toggle-control button{
    background: white;
    border: none;
    border-radius: 15px;
    height: 30px;
    width: 30px;
    font-size: x-small;
    font-weight: bolder;
    outline: none;
    cursor: pointer;
    box-shadow: lightyellow 1px 1px 1px;
    color: #6d0ea4;
  }
</style>
<style>
  @import "../../node_modules/@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";
  @import "../../node_modules/mapbox-gl/dist/mapbox-gl.css";

  .mapboxgl-canvas-container {
    position: absolute !important;
  }
</style>

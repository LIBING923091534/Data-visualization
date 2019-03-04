<template>
  <div class="control-bar">
    <div class="upload-container">
      <p class="control-bar-title">
        数据上传
        <el-button @click="clickUpload">开始上传</el-button>
        <input type="file" ref="upload-file" id="upload-file" @change="uploadFile()" accept=".txt" style="display: none"/>
        <input type="number" id="maxView" placeholder="最大显示数量..." v-model="maxView" style="margin-left: 25px; width: 100px; padding-left: 15px">
      </p>
    </div>
    <div class="summary-container">
      <p class="control-bar-title">数据总览</p>
      <div class="statistic-item-summary">
        <table class="summary-table">
          <tr>
            <td>轨迹总数</td>
            <td>{{ statisticSummary.pathCount }}</td>
            <td>车辆总数</td>
            <td>{{ statisticSummary.carCount }}</td>
          </tr>
          <tr>
            <td colspan="1">数据时间范围</td>
            <td colspan="3">{{ statisticSummary.startTime | dateFormat }} - {{ statisticSummary.endTime | dateFormat
              }}
            </td>
          </tr>
        </table>
      </div>
    </div>
    <div class="tools-container">
      <p class="control-bar-title">数据查询</p>
      <div class="tools-propSelect">
        <p>车辆属性</p>
        <div class="carProp-wrapper">
          <input type="text" id="carID" placeholder="车辆编号" v-model="carInfo.ID">
        </div>
      </div>
      <div class="tools-datetimepicker">
        <p>时间范围</p>
        <div class="datetimepicker-wrapper">
          <input id="datetimepicker1" type="text" onfocus="this.blur()">
          -
          <input id="datetimepicker2" type="text" onfocus="this.blur()">
        </div>
      </div>

      <div class="tools-areaSelect">
        <p>空间范围</p>
        <div class="areaSelect-wrapper">
          <input type="radio" name="area" :value="0" v-model="areaSelect.type"/>不限
          <input type="radio" name="area" :value="1" v-model="areaSelect.type"/>指定单个区域
          <input type="radio" name="area" :value="2" v-model="areaSelect.type"/>指定两个区域(起终点)
          <div class="tools-areaSelect-1" v-if="areaSelect.type===1">
            <span>指定区域范围:</span>
            <span v-if="!selectedArea[0].selected">
              <button data-type="1" @click="clickSelect(0,'manual')">手动选择</button>
              <button data-type="2" @click="clickSelect(0,'auto')">常用范围</button>
              <button data-type="3" @click="clickSelect(0,'file')">导入文件</button>
            </span>
            <span v-else>
              <span>已选择1个区域</span>
              <button @click="clickSelect(0,'delete')">取消选择</button>
            </span>
          </div>
          <div class="tools-areaSelect-2" v-if="areaSelect.type===2">
            <p>
              <span>指定起点范围:</span>
              <span v-if="!selectedArea[1].selected">
                <button data-type="1" @click="clickSelect(1,'manual')">手动选择</button>
                <button data-type="2" @click="clickSelect(1,'auto')">常用范围</button>
                <button data-type="3" @click="clickSelect(1,'file')">导入文件</button>
              </span>
              <span v-else>
                <span>已选择1个区域</span>
                <button @click="clickSelect(1,'delete')">取消选择</button>
              </span>
            </p>
            <p>
              <span>指定终点范围:</span>
              <span v-if="!selectedArea[2].selected">
                <button data-type="1" @click="clickSelect(2,'manual')">手动选择</button>
                <button data-type="2" @click="clickSelect(2,'auto')">常用范围</button>
                <button data-type="3" @click="clickSelect(2,'file')">导入文件</button>
              </span>
              <span v-else>
                <span>已选择1个区域</span>
                <button @click="clickSelect(2,'delete')">取消选择</button>
              </span>
            </p>
          </div>
        </div>
        <div class="areaSelect-button-wrapper">
          <button @click="clickStartSearch">查询</button>
        </div>
      </div>
    </div>
    <div class="statistic-container">
      <div class="statistic-item">
        <p class="control-bar-title">数据统计</p>
        <div class="statistic-item-result">
          轨迹总数:<span>{{ statisticInfo.count }}</span>
          平均车速:<span>{{ statisticInfo.avgSpeed }} KM/h</span>
          平均里程:<span>{{ statisticInfo.avgMile }} KM</span>
        </div>
      </div>
      <div class="statistic-item">
        <p>数据分布</p>
        <div id="statistic-item-info"></div>
      </div>
    </div>

  </div>
</template>

<script>
  import '../assets/datetimepicker/jquery.datetimepicker.full'
  import moment from 'moment'
  import echarts from "echarts"

  export default {
    name: "control-bar",
    data() {
      return {
        maxView:1000,
        carInfo:{ID:""},
        pickTime: {start: 0, end: 0},
        areaSelect: {
          type: 0,
          // area1: [],
          // area2: []
        },
        chart: null,
        chartOption: {
          title: [{
            text: '车速分布',
            textStyle: {
              color: 'white',
              fontSize: 15
            },
            x: 60,
            y: 0
          }, {
            text: '里程分布',
            textStyle: {
              color: 'white',
              fontSize: 15
            },
            x: 250,
            y: 0
          }],
          grid: [
            {x: '7%', y: '14%', width: '40%', height: '70%'},
            {x2: '7%', y: '14%', width: '40%', height: '70%'}],
          tooltip: {},
          legend: {
            data: ['销量1']
          },
          xAxis: [
            {
              gridIndex: 0,
              data: ["<5", "5-10", "10-20", "20-30", "30-40", ">40"],
              axisLine: {lineStyle: {color: 'white'}}
            },
            {
              gridIndex: 1,
              data: ["<2", "2-5", "5-8", "8-12", "12-16", ">16"],
              axisLine: {lineStyle: {color: 'white'}}
            }
          ],
          yAxis: [
            {gridIndex: 0, min: 0, axisLine: {lineStyle: {color: 'white'}}},
            {gridIndex: 1, min: 0, axisLine: {lineStyle: {color: 'white'}}},
          ],
          series: [
            {
              name: '车速统计',
              type: 'bar',
              xAxisIndex: 0,
              yAxisIndex: 0,
              itemStyle: {
                normal: {label: {show: true}},
                emphasis: {label: {show: true}}
              },
              data: []
            }
            ,
            {
              name: '里程统计',
              type: 'bar',
              xAxisIndex: 1,
              yAxisIndex: 1,
              itemStyle: {
                normal: {label: {show: true}},
                emphasis: {label: {show: true}}
              },
              // tooltip: {
              //   trigger: 'item',
              //   formatter: "{c} 公里"
              // },
              data: []
            }
          ]
        }
      }
    },
    props: {
      selectedArea: Array,
      statisticSummary: Object,
      statisticInfo: Object
    },
    watch: {
      statisticInfo: {
        // immediate:true,
        deep: true,
        handler: function (v) {
          // 指定图表的配置项和数据
          if (this.chart !== null) {
            this.chartOption.series[0].data = v.barSpeed
            this.chartOption.series[1].data = v.barMile
            // 使用刚指定的配置项和数据显示图表。
            this.chart.setOption(this.chartOption);
          }
        }
      }
    },
    mounted() {
      $.datetimepicker.setLocale('zh');
      $('#datetimepicker1').datetimepicker({
        value: '2014/01/31 00:00',
        onChangeDateTime: (e) => {
          this.pickTime.start = parseInt(moment(e).format("X"))
        }
      });
      $('#datetimepicker2').datetimepicker({
        value: '2014/02/01 00:00',
        onChangeDateTime: (e) => {
          this.pickTime.end = parseInt(moment(e).format("X"))
        }
      });
      this.pickTime.start = parseInt(moment('2014-01-31 00:00').format("X"))
      this.pickTime.end = parseInt(moment('2014-02-01 00:00').format("X"))
      this.initEcharts()
    },
    methods: {
      clickSelect(num, type) {
        this.$emit('clickSelect', num, type)
      },
      clickStartSearch() {
        // 点击开始查询，传递选择的时间以及区域查询的类型
        this.$emit('clickSearch',this.maxView, this.carInfo, this.pickTime, this.areaSelect.type)
      },
      clickUpload(){
        this.$refs["upload-file"].click()
      },
      uploadFile(){
        let selectedFile = document.getElementById('upload-file').files[0];
        var fd = new FormData();
        var xhr = new XMLHttpRequest();

        fd.append("file", selectedFile);
        xhr.open("post", "/taxiHttpServer/Search", true);

        const loading = this.$loading({
          lock: true,
          text: '数据处理中  \n  该过程可能比较漫长，请您耐心等待',
          spinner: 'el-icon-loading',
          background: 'rgba(0, 0, 0, 0.7)'
        });

        xhr.onreadystatechange=function()
        {
          loading.close()
          if (xhr.readyState===4)
          {
            if(xhr.status===200 && xhr.responseText.toLowerCase().trim() === 'success'){
              location.reload()
            }else {
              alert("文件上传失败")
            }
          }
        }
        xhr.send(fd);
      },
      initEcharts() {
        this.chart = echarts.init(document.getElementById('statistic-item-info'), 'light')

        // 指定图表的配置项和数据
        this.chartOption.series[0].data = this.statisticInfo.barSpeed
        this.chartOption.series[1].data = this.statisticInfo.barMile

        // 使用刚指定的配置项和数据显示图表。
        this.chart.setOption(this.chartOption);
      }
    },
    filters: {
      dateFormat(timestamp) {

        return moment.unix(timestamp).isValid() ? moment.unix(timestamp).format("YYYY-MM-DD hh:mm:ss") : '加载中...'
      }
    }
  }
</script>

<style scoped>
  .control-bar {
    position: fixed;
    right: 0;
    top: 0;
    height: 100%;
    width: 400px;
    background: rgba(0, 0, 0, 0.48);
    color: white;
    text-align: left;
    padding: 2px;
    z-index: 99;
  }

  .control-bar p {
    margin: 5px;
  }

  .control-bar-title {
    font-size: large;
    font-weight: bolder;
    margin-top: 0;
  }
  .summary-container .summary-table{
    margin: 0 auto;
  }

  .carProp-wrapper, .datetimepicker-wrapper{
    text-align: center;
  }
  .areaSelect-button-wrapper{
    margin-top: 5px;
  }
  .control-bar input[type='text'],.control-bar input[type='number'] {
    text-align: center;
    background: transparent;
    border: 1px solid white;
    color: white;
    cursor: pointer;
    line-height: 18px;
  }
  .control-bar input[id="carID"]{
    width: 89%;
  }

  .control-bar button{
    border: none;
    border-radius: 5px;
    font-weight: bolder;
    cursor: pointer;
    padding: 5px;
    margin-left: 10px;
  }

  .control-bar button[data-type='1']{
    background: #25a7bc;
    color: #f5fff3;
  }
  .control-bar button[data-type='2']{
    background: #30bc66;
    color: #fff6fe;
  }
  .control-bar button[data-type='3']{
    background: #a958bc;
    color: #fff9fb;
  }

  .areaSelect-button-wrapper button{
    width: 80px;
    height: 30px;
    background: transparent;
    color: #f5fff3;
    border: 1px solid white;
  }
  .upload-container, .summary-container, .tools-container, .statistic-container {
    border: 0.5px dashed white;
    border-radius: 10px;
    margin: 8px;
    padding: 5px;
  }

  .statistic-item-summary table {
    border-collapse: collapse;
  }

  .statistic-item-summary table td {
    border: 1px solid whitesmoke;
    margin: 0;
    padding: 5px;
  }

  .statistic-item-summary, .statistic-item-result {
    font-weight: lighter;
    font-size: small;
    text-align: center;
  }

  .statistic-item-result span {
    color: aqua;
  }

  #statistic-item-info {
    width: 380px;
    height: 185px;
  }
</style>
<style>
  @import "../assets/datetimepicker/jquery.datetimepicker.css";
</style>

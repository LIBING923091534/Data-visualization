<template>
    <div class="timeline-container">
      <div class="timeline-box" id="timeline-box">

      </div>
    </div>
</template>

<script>
  import moment from 'moment'
  import echarts from "echarts"
    export default {
        name: "timeline-bar",
      data(){
          return{
            chart:null,
            chartOption:{
              xAxis: {
                show: true,
                type: 'time',
                axisLine: {lineStyle: {color: 'white'}},
                axisPointer: {
                  snap: false,
                  lineStyle: {
                    color: '#004E52',
                    opacity: 0.5,
                    width: 2
                  },
                  label: {
                    show: true,
                    formatter: function (params) {
                      return echarts.format.formatTime('yyyy-MM-dd hh:mm', params.value);
                    },
                    backgroundColor: '#004E52'
                  },
                  handle: {
                    show: true,
                    color: '#004E52'
                  }
                },
                splitLine: {
                  show: false
                }
              },
              yAxis: {
                show: false,
                type: 'value'
              },
              grid: {
                top: 40,
                left: 20,
                right: 25,
                height: 70
              },
              series: [{
                data: [
                  // [1521611548000,932],
                  // [1521611558000,1901],
                  // [1521611568000,534],
                  // [1521611578000,1090],
                  // [1521611598000,1330],
                  // [1521611608000,132],
                ],
                type: 'line',
                smooth: true,
                symbol: 'circle',
                symbolSize: 5,
                sampling: 'average',
                itemStyle: {
                  normal: {
                    show: false,
                    color: '#a0adb0'
                  }
                },
                areaStyle: {
                  normal: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{
                      offset: 0,
                      color: 'rgba(40, 182, 252, 0.85)'
                    }, {
                      offset: 1,
                      color: 'rgba(28, 159, 255, 0.01)'
                    }])
                  }
                },
              }]
            }
          }
      },
      mounted(){
          this.initEcharts()
      },
      props:{
          timeBarList:Array
      },
      watch:{
        timeBarList:{
          deep: true,
          handler:function (v) {
            if(this.chart !== null){
              this.chartOption.series[0].data = v
              this.chart.setOption(this.chartOption);
            }
          }
        }
      },
      methods:{
          initEcharts(){
            this.chart = echarts.init(document.getElementById('timeline-box'), 'light')

            this.chartOption.series[0].data = this.timeBarList
            this.chart.setOption(this.chartOption);
          }
      }
    }
</script>

<style scoped>
.timeline-box{
  width: 1100px;
  height: 150px;
  position: fixed;
  bottom: 15px;
  left: 40px;
}
</style>

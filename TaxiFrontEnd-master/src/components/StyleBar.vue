<template>
  <div class="style-bar">
    <div class="style-bar-container" :class="{'style-bar-show':showStyleBar}">
      <div class="style-item-container">
        <p>地图风格</p>
        <el-radio-group v-model="styleVal.mapStyle" size="mini" style="padding-left: 30px">
          <el-radio label="1" border>黑夜模式</el-radio>
          <el-radio label="2" border>明亮模式</el-radio>
        </el-radio-group>
      </div>
      <div class="style-item-container">
        <p>车辆标注
        <el-switch
          v-model="styleVal.effect" style="float: right;height: 32px"
         >
        </el-switch></p>
      </div>
      <div class="style-item-container">
        <p>轨迹颜色
          <el-color-picker style="float: right"
            v-model="styleVal.color"
            size="medium"
            :predefine="predefineColors">
          </el-color-picker></p>
      </div>
      <div class="style-item-container">
        <p style="margin-bottom: 0px">轨迹粗细</p>
        <div class="block">
          <span class="demonstration">单条轨迹绘制时的粗细</span>
          <el-slider v-model="styleVal.width" :min=0.1 :max=5.0 :step=0.05></el-slider>
        </div>
      </div>
      <div class="style-item-container">
        <p style="margin-bottom: 0px">轨迹透明度</p>
        <div class="block">
          <span class="demonstration">单条轨迹绘制时的透明度</span>
          <el-slider v-model="styleVal.opacity" :min=0 :max=1 :step=0.01></el-slider>
        </div>
      </div>
      <div class="style-item-buttons">
        <el-button type="primary" round @click="defaultClick">默认值</el-button>
        <el-button type="danger" round @click="confirmClick">确定</el-button>
      </div>
    </div>

    <div class="style-bar-handle" @click="toggleStyleBar">
      <p>样式选择</p>
    </div>
  </div>
</template>

<script>
  export default {
    name: "style-bar",
    data() {
      return {
        showStyleBar: true,

        predefineColors: [
          '#ff4500',
          '#ff8c00',
          '#ffd700',
          '#90ee90',
          '#00ced1',
          '#1e90ff',
          '#c71585'
        ],
        styleVal:{
          mapStyle:'1',
          effect: false,
          color: '#1D6AB3',
          width:0.8,
          opacity:0.6
        }

      }
    },
    methods: {
      toggleStyleBar() {
        this.showStyleBar = !this.showStyleBar;
      },
      defaultClick(){
        this.styleVal={
          mapStyle:'1',
          effect: false,
            color: '#1D6AB3',
            width:0.8,
            opacity:0.6
        }
      },
      confirmClick(){
        this.$emit('changeStyle',this.styleVal)
      }
    }
  }
</script>

<style scoped>
  .style-bar {
    position: fixed;
    left: 0;
    top: 50%;
    transform: translateY(-60%);
    margin-left: -2px;

  }

  .style-bar-container {
    width: 300px;
    height: 520px;
    background: rgba(107, 107, 112, 0.78);
    position: relative;
    transition: all 0.4s ease-out;
    text-align: left;
    color: white !important;
    font-size: medium;
    border: 1px grey solid;
  }

  .style-bar-container p{
    color: #bff5ed;
    font-size: large;
    line-height: 32px;
  }
  .style-item-container .color-span{
    line-height: 42px;
    display: inline-block;
    vertical-align: bottom;
  }
  .style-item-container{
    margin: 20px 20px 10px 20px;
  }
  .style-item-buttons{
    margin-top: 30px;
    text-align: center;
  }

  .style-bar-show {
    margin-left: -300px;
  }

  .style-bar-handle {
    position: absolute;
    right: 0;
    top: 50%;
    transform: translateY(-50%) translateX(100%);
    width: 30px;
    height: 150px;
    background: rgba(245, 255, 252, 0.53);
    border-radius: 0 5px 5px 0;
    cursor: pointer;
    box-shadow: inset lightyellow 0px 0px 18px 3px;
  }
  .style-bar-handle p{
    margin-top: 30px;
    color: rgba(244, 255, 252, 0.97);
  }
  .el-radio{
    color: white;
  }
</style>

// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import App from './App'
import router from './router'
import VueResource from 'vue-resource'
import { Button, ColorPicker, Slider, Radio, RadioGroup, RadioButton, Loading, Switch  } from 'element-ui'

Vue.config.productionTip = false
Vue.use(VueResource)
Vue.use(Button)
Vue.use(ColorPicker)
Vue.use(Slider)
Vue.use(Radio)
Vue.use(RadioGroup)
Vue.use(RadioButton)
Vue.use(Loading)
Vue.use(Switch)

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  components: { App },
  template: '<App/>'
})

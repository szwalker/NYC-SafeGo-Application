import Vue from 'vue'
import App from './App.vue'

import VueMaterial from 'vue-material'
import 'vue-material/dist/vue-material.min.css'
import 'vue-material/dist/theme/default.css'
import * as VueGoogleMaps from "vue2-google-maps";

Vue.use(VueMaterial)

Vue.use(VueGoogleMaps, {
  load: {
    key: "AIzaSyD2BAMoxNm3FkWZL7s61iLQSrJ0QY4NbVA",
    libraries: "places" // necessary for places input
  }
});
Vue.config.productionTip = false

new Vue({
  render: h => h(App),
}).$mount('#app')

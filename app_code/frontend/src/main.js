import Vue from 'vue'
import App from './App.vue'

import VueMaterial from 'vue-material'
import 'vue-material/dist/vue-material.min.css'
import 'vue-material/dist/theme/default.css'
import * as VueGoogleMaps from "vue2-google-maps";

Vue.use(VueMaterial)

Vue.use(VueGoogleMaps, {
  load: {
    key: "", // pls contact me for key
    libraries: "places" // necessary for places input
  }
});
Vue.config.productionTip = false

new Vue({
  render: h => h(App),
}).$mount('#app')

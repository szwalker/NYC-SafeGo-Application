<template>
  <div id="app">
    <div class="horizontal-box">
      <div style="width:500px; height: 500px">
        <google-map-loader :safePath.sync="safePath" :shortPath.sync="shortPath"/>
      </div>
      <div class="vertical-box">
        <Search v-on:search-route = "searchRoute"/>
        <Route :routeShortDataList.sync="routeShortDataList" :routeSafeDataList.sync = "routeSafeDataList" />
      </div>
    </div>
  </div>
</template>
cd
<script>
  import Search from './components/Search.vue';
  import Route from "@/components/Route";
  import GoogleMapLoader from "@/components/GoogleMapLoader";
  import axios from 'axios';
  export default {
    name: 'app',
    components: {
      GoogleMapLoader,
      Route,
      Search
    },
    data() {
      return {
        safePath: [],
        shortPath:[],
        routeSafeDataList:[],
        routeShortDataList:[]
      }
    },

    methods: {
      async geolookup(loc) {
        let result = await axios({ method: "GET", "url": "https://maps.googleapis.com/maps/api/geocode/json",
          params:{address: loc, key:"AIzaSyD2BAMoxNm3FkWZL7s61iLQSrJ0QY4NbVA"}, "headers": { "content-type": "application/json" } });
        if (result['data']['status'] === 'OK') {
          let loc = result['data']['results'][0]['geometry']['location'];
          return loc;
        }
        else {
          return null;
        }
      },
      async searchRoute(src, dst) {
        console.log(src, dst)
        let start_lat = (await this.geolookup(src))['lat'];

        let start_lng = (await this.geolookup(src)).lng;

        let dst_lat = (await this.geolookup(dst)).lat;
        let dst_lng = (await this.geolookup(dst)).lng;

        console.log(start_lat, start_lng, dst_lat, dst_lng);

        let result = (await axios({ method: "GET", "url": "http://127.0.0.1:5000/index?startLat=" + start_lat +"&startLng=" + start_lng + "&endLat=" + dst_lat + "&endLng=" + dst_lng}))['data']
        console.log(result)
          // let result = res.data['safest']

        // let result = {
        //   shortest:[
        //     {"sid" : 170875, "polygons" : [ { "lat" : 40.85951850445461, "lng" : -73.92252465335248 }, { "lat" : 40.859437706451104, "lng" : -73.92239298148154 }, { "lat" : 40.85862504145975, "lng" : -73.921760963302 }, { "lat" : 40.85799553553433, "lng" : -73.92173050178266 }, { "lat" : 40.85786022354022, "lng" : -73.92181981024588 } ], "streetName" : "DRIVEWAY", "streetLen" : 675.330704056, "neighbors" : [ 170875, 161949 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6339630484580994 },
        //     {"sid" : 183608, "polygons" : [ { "lat" : 40.85044732520501, "lng" : -73.93666142761384 }, { "lat" : 40.85136331508886, "lng" : -73.93620370000076 }, { "lat" : 40.85146696770021, "lng" : -73.93615190296994 }, { "lat" : 40.852335152313174, "lng" : -73.93571740885848 } ], "streetName" : "BENNETT AVE", "streetLen" : 735.721079105, "neighbors" : [ 26790, 26791, 83025, 83026, 183608, 73151 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.635206051170826 },
        //     {"sid" : 488, "polygons" : [ { "lat" : 40.70454591413195, "lng" : -74.01006968341342 }, { "lat" : 40.70477524791872, "lng" : -74.00974639311217 } ], "streetName" : "STONE ST", "streetLen" : 122.538700052, "neighbors" : [ 96896, 488, 489, 109550, 825, 103198 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6345022320747375 },
        //     {"sid" : 79723, "polygons" : [ { "lat" : 40.707857835737876, "lng" : -74.0068377870997 }, { "lat" : 40.70822712625459, "lng" : -74.00653116871753 } ], "streetName" : "GOLD ST", "streetLen" : 159.149458558, "neighbors" : [ 79744, 79721, 79722, 79723, 79724, 79725, 79743 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6170931234955788 },
        //     {"sid" : 179568, "polygons" : [ { "lat" : 40.79621698487826, "lng" : -73.94968682996546 }, { "lat" : 40.79600447773409, "lng" : -73.94918568582632 }, { "lat" : 40.795941273538325, "lng" : -73.94904329476765 }, { "lat" : 40.795536417729416, "lng" : -73.94808516529754 } ], "streetName" : "E  109 ST", "streetLen" : 508.092565157, "neighbors" : [ 89924, 3535, 179568, 3536, 181590, 2010 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6392536535859108 },
        //     {"sid" : 3904, "polygons" : [ { "lat" : 40.8467105236121, "lng" : -73.93191507377053 }, { "lat" : 40.84680263276957, "lng" : -73.93185010295767 } ], "streetName" : "AMSTERDAM AVE", "streetLen" : 38.0698690576, "neighbors" : [ 3904, 3905, 83048, 28012, 28013, 3903 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6212980225682259 },
        //     {"sid" : 1121, "polygons" : [ { "lat" : 40.749052196220944, "lng" : -73.99574855573138 }, { "lat" : 40.74967168209748, "lng" : -73.99529821527037 } ], "streetName" : "EIGHTH AVE", "streetLen" : 257.894368484, "neighbors" : [ 1120, 1121, 1122, 81163, 1388, 1389, 81167 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.5802588351070881 },
        //     {"sid" : 21835, "polygons" : [ { "lat" : 40.76085809262594, "lng" : -73.96106754453669 }, { "lat" : 40.75991278601397, "lng" : -73.95880955474563 } ], "streetName" : "E  61 ST", "streetLen" : 714.075355588, "neighbors" : [ 21834, 21835, 21836, 2672, 2673, 19509, 19510 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.5740101709961891 },
        //     {"sid" : 132316, "polygons" : [ { "lat" : 40.76790621913489, "lng" : -73.94324542439227 }, { "lat" : 40.76819089133, "lng" : -73.94299356982305 }, { "lat" : 40.76820822445321, "lng" : -73.94297950727766 }, { "lat" : 40.768225337386525, "lng" : -73.9429649863737 }, { "lat" : 40.768242223934166, "lng" : -73.94295001284526 }, { "lat" : 40.76825887622521, "lng" : -73.94293459264823 }, { "lat" : 40.768275287394424, "lng" : -73.94291873283939 }, { "lat" : 40.768291451916205, "lng" : -73.94290243937267 }, { "lat" : 40.768307362087384, "lng" : -73.94288571864459 }, { "lat" : 40.76832301171293, "lng" : -73.94286857793176 }, { "lat" : 40.768338394430295, "lng" : -73.94285102451099 }, { "lat" : 40.768353504043986, "lng" : -73.94283306477756 }, { "lat" : 40.768368334191806, "lng" : -73.94281470666934 }, { "lat" : 40.76838287918129, "lng" : -73.94279595746262 }, { "lat" : 40.76839713181261, "lng" : -73.94277682509605 }, { "lat" : 40.76841108790137, "lng" : -73.9427573175057 }, { "lat" : 40.76852492296421, "lng" : -73.94258900640158 } ], "streetName" : "ROOSEVELT IS GREENWAY", "streetLen" : 291.270268205, "neighbors" : [ 132313, 132314, 132315, 132316, 146174 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.541178360581398 },
        //     {"sid" : 474, "polygons" : [ { "lat" : 40.73572292371414, "lng" : -74.00679907002781 }, { "lat" : 40.736431696922914, "lng" : -74.006620319663 } ], "streetName" : "GREENWICH ST", "streetLen" : 262.936961672, "neighbors" : [ 79942, 79943, 79733, 79734, 473, 474, 475 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.585627943277359 }
        //   ],
        //   safest: [
        //     {"sid" : 2289, "polygons" : [ { "lat" : 40.77261802984363, "lng" : -73.98230415717573 }, { "lat" : 40.77279664646697, "lng" : -73.98219354226512 }, { "lat" : 40.77299071196187, "lng" : -73.98207933930414 } ], "streetName" : "COLUMBUS AVE", "streetLen" : 149.385237812, "neighbors" : [ 2824, 2825, 4269, 4270, 2288, 2289, 2290, 2836, 2837 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6033021584153175 },
        //     {"sid" : 16526, "polygons" : [ { "lat" : 40.731288523324885, "lng" : -73.9903901000268 }, { "lat" : 40.73199633440485, "lng" : -73.99021190972442 } ], "streetName" : "FOURTH AVE", "streetLen" : 262.563940176, "neighbors" : [ 3980, 3981, 16526, 16527, 4015, 79797, 183615 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6127470433712006 },
        //     {"sid" : 132675, "polygons" : [ { "lat" : 40.80294528104552, "lng" : -73.93044986873302 }, { "lat" : 40.803267496621075, "lng" : -73.93061032684781 }, { "lat" : 40.803515447791135, "lng" : -73.9307614339016 }, { "lat" : 40.80376704707199, "lng" : -73.93093988106959 }, { "lat" : 40.80390305151986, "lng" : -73.93103634377337 } ], "streetName" : "HARLEM RIV DR", "streetLen" : 385.493252055, "neighbors" : [ 132674, 132675, 24976, 24977, 24886 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6619542986154556 },
        //     {"sid" : 172572, "polygons" : [ { "lat" : 40.80723724520665, "lng" : -73.9332590953999 }, { "lat" : 40.807855844978825, "lng" : -73.93184564493095 } ], "streetName" : "3 AVE  BRIDGE", "streetLen" : 451.556625695, "neighbors" : [ 172572, 79364 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6425319314002991 },
        //     {"sid" : 146083, "polygons" : [ { "lat" : 40.76386277208094, "lng" : -73.94662141408685 }, { "lat" : 40.76387990891827, "lng" : -73.94665386554145 } ], "streetName" : "ROOSEVELT IS BRG", "streetLen" : 10.945017143, "neighbors" : [ 146144, 146146, 146083, 132324, 146147, 146187, 77232, 30712, 77243 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.5728958025574684 },
        //     {"sid" : 2988, "polygons" : [ { "lat" : 40.79591917001086, "lng" : -73.97073269636819 }, { "lat" : 40.79655201203386, "lng" : -73.97026645527687 } ], "streetName" : "BROADWAY", "streetLen" : 264.245842236, "neighbors" : [ 96033, 2987, 2988, 2989, 23375, 23376, 176473 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.640359990298748 },
        //     {"sid" : 186337, "polygons" : [ { "lat" : 40.846097497454146, "lng" : -73.93016615631757 }, { "lat" : 40.84673248130911, "lng" : -73.93038657505265 } ], "streetName" : "HIGH BRIDGE PARK PATH", "streetLen" : 239.251295406, "neighbors" : [ 186336, 186337, 186339, 67710, 186333, 186334, 186335 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6283651143312454 },
        //     {"sid" : 175952, "polygons" : [ { "lat" : 40.79108027732964, "lng" : -73.95533156961466 }, { "lat" : 40.790666650874236, "lng" : -73.95471310702426 }, { "lat" : 40.790630901290776, "lng" : -73.95465330250971 } ], "streetName" : "E  MEADOW PATH", "streetLen" : 249.186801203, "neighbors" : [ 175940, 175950, 175952, 175953, 175964 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6054141744971275 },
        //     {"sid" : 21810, "polygons" : [ { "lat" : 40.810947189809916, "lng" : -73.96305844719707 }, { "lat" : 40.81225155784538, "lng" : -73.9621056178172 } ], "streetName" : "CLAREMONT AVE", "streetLen" : 543.516577828, "neighbors" : [ 81602, 178246, 72743, 72742, 183558, 21810, 21812 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.6166369765996933 },
        //     {"sid" : 145191, "polygons" : [ { "lat" : 40.763960790379485, "lng" : -73.94665967061823 }, { "lat" : 40.764156823483155, "lng" : -73.94671857637515 }, { "lat" : 40.764115909442694, "lng" : -73.94696389536337 }, { "lat" : 40.764144870119125, "lng" : -73.94702796236948 }, { "lat" : 40.764497475408014, "lng" : -73.94671452337374 }, { "lat" : 40.76455307277968, "lng" : -73.9468376467805 } ], "streetName" : "PEDESTRIAN AND BIKE PATH LINK", "streetLen" : 358.205423907, "neighbors" : [ 146144, 146145, 145191, 145192, 144534, 146175 ], "hasHomeless" : false, "hasDrinking" : false, "hasDamagedSteetlight" : false, "crimeIndex" : 0.5716035850346088 }
        //     ]};

        this.safePath = result['safest'];
        this.shortPath = result['shortest'];

        this.routeSafeDataList = []
        result['safest'].forEach((ele) => {
          this.routeSafeDataList.push({
            sid: ele.sid,
            streetName: ele.streetName,
            streetLen: ele.streetLen,

          });
        })

        this.routeShortDataList = []
        result['shortest'].forEach((ele) => {
          this.routeShortDataList.push({
            sid: ele.sid,
            streetName: ele.streetName,
            streetLen: ele.streetLen,

          });
        })

      }
    }
  }
</script>

<style>
  .horizontal-box {
    display: flex;
    flex-direction: row;
    justify-content: space-around;
  }

  .vertical-box {
    display: flex;
    width: 500px;
    flex-direction: column;
  }
  #app {
    font-family: 'Avenir', Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: center;
    color: #2c3e50;
    margin-top: 60px;

  }
</style>

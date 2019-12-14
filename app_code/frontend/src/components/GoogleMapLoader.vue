<template>
    <div>
        <div class="google-map"></div>
        <gmap-map
                :center="center"
                :zoom="12"
                style="width:100%;  height: 500px;"
        >
            <div v-for="p in safepolygons" :key="p.id">
                <gmap-polyline v-bind:path="p.path" v-bind:options = "{ strokeColor: '#4ba82e' }"></gmap-polyline>
            </div>
            <div v-for="p in shortpolygons" :key="p.id">
                <gmap-polyline v-bind:path="p.path" v-bind:options = "{ strokeColor: '#ff0000' }"></gmap-polyline>
            </div>
        </gmap-map>

    </div>
</template>

<script>
    export default {
        name: "GoogleMapLoader",
        props: {
            safePath: Array,
            shortPath: Array
        },
        data() {
            return {
                center: { lat: 40.7678, lng: -73.978 },
                places: [],
                currentPlace: null
            };
        },

        mounted() {
            this.geolocate();
        },
        methods: {
            // receives a place object via the autocomplete component
            setPlace(place) {
                this.currentPlace = place;
            },
            geolocate: function() {
                navigator.geolocation.getCurrentPosition(position => {
                    this.center = {
                        lat: position.coords.latitude,
                        lng: position.coords.longitude
                    };
                });
            }

        },
        computed: {
            safepolygons() {
                return this.safePath.map((ele) => {
                    // console.log(typeof ele.crimeIndex)
                    return {
                        path: ele.polygons
                        // color: "#" + (parseInt((ele.crimeIndex * 255)).toString(16) + (parseInt((1-ele.crimeIndex) * 255) ).toString(16) + "00")
                        // color: (0.5 * 255).toString(16) + ((1-0.5) * 255 ).toString(16) + "00",
                    }
                })
            },

            shortpolygons() {
                return this.shortPath.map((ele) => {
                    // console.log(typeof ele.crimeIndex)
                    return {
                        path: ele.polygons
                        // color: "#" + (parseInt((ele.crimeIndex * 255)).toString(16) + (parseInt((1-ele.crimeIndex) * 255) ).toString(16) + "00")
                        // color: (0.5 * 255).toString(16) + ((1-0.5) * 255 ).toString(16) + "00",
                    }
                })
            }
        }
    }
</script>


<style scoped>
    #map {
        height: 100vh;
        width: 100%;
    }
</style>
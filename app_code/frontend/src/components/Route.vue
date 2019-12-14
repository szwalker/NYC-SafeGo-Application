<template>
    <div>
        <md-card>
            <md-content class="md-scrollbar" style="max-height:400px;overflow: auto">
                <md-list class="md-scrollbar" style="max-height:400px;">
                    <md-list-item>
                        <span class="md-list-item-text"><b>Safest Route</b></span>
                    </md-list-item>
                    <md-list-item v-bind:routeSafeDataList="routeSafeDataList" v-for="item in routeSafeDataList" :key = "item.id">
                        <span>{{ item.streetName }} walks {{ parseInt(item.streetLen) }} meters</span>
                    </md-list-item>
                    <md-list-item>
                        <span class="md-list-item-text"><b>Shortest Route</b></span>
                    </md-list-item>
                    <md-list-item v-bind:routeShortDataList="routeShortDataList" v-for="item in routeShortDataList" :key = "item.id">
                        <span>{{ item.streetName }} walks {{ parseInt(item.streetLen) }} meters</span>
                    </md-list-item>
                </md-list>
            </md-content>
        </md-card>

    </div>
</template>

<script>
    // import axios from 'axios';
    export default {
        name: "Route",
        props: {
            routeSafeDataList: {
                type: Array,
                default: () => []
            },
            routeShortDataList: {
                type: Array,
                default: () => []
            },
        },
        data() {
            return {}
        },
        methods: {
        }
    }
</script>

<style scoped>
    .md-card {
        width: 400px;
        margin: 4px;
        display: inline-block;
        vertical-align: top;
    }
</style>
<template>
    <div>
        <md-card>
            <md-card-content>
                <md-field>
                    <md-icon>trip_origin</md-icon>
                    <label>From</label>
                    <md-input v-model="input.start_point"></md-input>
                </md-field>

                <md-field>
                    <md-icon>where_to_vote</md-icon>
                    <label>To</label>
                    <md-input v-model="input.destination"></md-input>
                    <md-button class="search-button" v-on:click="onClick()">
                        <md-icon>search</md-icon>
                    </md-button>
                </md-field>
            </md-card-content>
        </md-card>
    </div>
</template>

<script>
    // import axios from "axios";
    export default {
        name: "Search",
        data() {
            return {
                input:{start_point:null, destination:null}
            }
        },



        methods: {
            onClick() {
                console.log(this.input.start_point)
                console.log(this.input.destination)
                this.$emit("search-route", this.input.start_point, this.input.destination)
            }
        }
    }
</script>

<style scoped>
    .md-field:last-child {
        margin-bottom: 40px;
    }
    .md-card {
        width: 400px;
        margin: 4px;
        display: inline-block;
        vertical-align: top;
    }
</style>
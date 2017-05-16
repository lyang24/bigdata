$(function () {

    var data_points = [];

    $("#chart").height($(window).height() - $("#header").height() * 2);

    $(document.body).on('click', '.stock-label', function () {
        "use strict";
        var symbol = $(this).text();
        $.ajax({
            url: 'http://localhost:5000/' + symbol,
            type: 'DELETE'
        });

        $(this).remove();
        var i = getSymbolIndex(symbol, data_points);
        data_points.splice(i, 1);
        console.log(data_points);
    });

    $("#add-stock-button").click(function () {
        "use strict";
        var symbol = $("#stock-symbol").val();

        $.ajax({
            url: 'http://localhost:5000/' + symbol,
            type: 'POST'
        });

        $("#stock-symbol").val("");
        data_points.push({
            values: [],
            key: symbol
        });

        $("#stock-list").append(
            "<a class='stock-label list-group-item small'>" + symbol + "</a>"
        );

        console.log(data_points);
    });

    function getSymbolIndex(symbol, array) {
        "use strict";
        for (var i = 0; i < array.length; i++) {
            if (array[i].key == symbol) {
                return i;
            }
        }
        return -1;
    }

    var chart = nv.models.lineChart()
        .interpolate('monotone')
        .margin({
            bottom: 100
        })
        .useInteractiveGuideline(true)
        .showLegend(true)
        .color(d3.scale.category10().range());

    chart.xAxis
        .axisLabel('Time')
        .tickFormat(function(d) { return d3.time.format('%H:%M:%SZ')(new Date(d)); })
        // .tickFormat(formatDateTick); //question??????

    chart.yAxis
        .axisLabel('Price');

    nv.addGraph(loadGraph);

    function loadGraph() {
        "use strict";
        d3.select('#chart svg')
            .datum(data_points)
            .transition()
            .duration(5)
            .call(chart);

        nv.utils.windowResize(chart.update);
        return chart;
    }

    function newDataCallback(message) {
        "use strict"
        var parsed = JSON.parse(message)[0];   
        var timestamp = parsed["LastTradeDateTime"]; //timestamp??
        var average = parsed["LastTradePrice"];
        var symbol = parsed["StockSymbol"];
        var point = {};


        var i = getSymbolIndex(symbol, data_points);

        var temp = formatDateTick(timestamp)
        console.log(data_points)
        console.log(temp)

        point.x = Date.parse(timestamp);
        point.y = average;

        data_points[i].values.push(point);
        if (data_points[i].values.length > 100) {
            data_points[i].values.shift();
        }
        loadGraph();
    }

    function formatDateTick(time) {
        "use strict"
        var date = new Date(Date.parse(time));
        return d3.time.format("%Y-%m-%dT%H:%M:%SZ")(date);

        // var date = new Date(time);
        // return d3.time.format("%Y-%m-%dT%H:%M:%SZ")(date);
    }

    var socket = io();

    // - Whenever the server emits 'data', update the flow graph
    socket.on('data', function (data) {
    	newDataCallback(data);
    });
});
<!DOCTYPE html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Waterlogging</title>
	<script type="text/javascript" src="http://api.map.baidu.com/api?v=2.0&ak=wDsjWTmWSyR6kQCZyllA7UHz"></script>
	<!-- 加载百度地图样式信息窗口 -->
	<script type="text/javascript" src="http://api.map.baidu.com/library/SearchInfoWindow/1.5/src/SearchInfoWindow_min.js"></script>
	<link rel="stylesheet" href="http://api.map.baidu.com/library/SearchInfoWindow/1.5/src/SearchInfoWindow_min.css" />
	<script type="text/javascript" src="http://api.map.baidu.com/library/Heatmap/2.0/src/Heatmap_min.js"></script>
	<script src="http://d3js.org/d3.v3.min.js"></script>
    <!-- Bootstrap -->
    <link href="http://cdn.bootcss.com/bootstrap/3.3.4/css/bootstrap.min.css" rel="stylesheet">
	<style>
		body{
			padding-top: 70px;
			margin:0px;
			padding:0px
		}  
		.main-body{
			margin-top: 100px;
		}
		.main-navigation .menu li.nav-current{
			border-bottom: 3px solid #FFFFFF;
			margin-bottom: -2px;
		}
		.introduction{
			margin-top: 50px;
		}
		.main-data{
			margin-top: 70px;
		}
		.chinese{
			text-indent:2em;
			margin-bottom:15px;
			font-size:21px;
			font-weight:200;
		}
		#map_canvas {
			height:450px;
			width:1000px;
			margin-top:20px;
			margin-bottom:60px;
			margin-left:180px;
			overflow: hidden;
		}
		.dailychange {
			height:450px;
			width:1000px;
			margin-top:50px;
			margin-bottom:20px;
			margin-left:180px;
			overflow: hidden;
		}
		.blankfora{
			visibility:hidden;
			margin-bottom:60px;
		}
		.citychoose{
			margin-left:30px;
		}
		#transferGraph{
			height:450px;
			width:1000px;
			margin-top:20px;
			margin-left:180px;
			margin-bottom:20px;
			overflow: hidden;
		}
		#pointLayerOpen{
			margin-left:300px;
		}
		#figure{
			margin-left:180px;
			margin-bottom:100px;
		}
		#search{
			width:240px;
			display:inline;
		}
		path {  stroke: #fff; }
		path:hover {  opacity:0.9; }
		rect:hover {  fill:blue; }
		.axis {  font: 10px sans-serif; }
		.frameChart tr{    border-bottom:1px solid grey; }
		.frameChart tr:first-child{    border-top:1px solid grey; }

		.axis path,
		.axis line {
			fill: none;
			stroke: #000;
			shape-rendering: crispEdges;
		}

		.x.axis path {  display: none; }
		.frameChart{
			margin-bottom:76px;
			display:inline-block;
			border-collapse: collapse;
			border-spacing: 0px;
		}
		.frameChart td{
			padding:4px 5px;
			vertical-align:bottom;
		}
		.frameFreq, .framePerc{
			align:right;
			width:50px;
		}
	</style>

  </head>
  <body>

	 <nav class="main-navigation navbar navbar-inverse navbar-fixed-top">
		<div class="container">
			<div class="row">
				<div class="col-md-12">
					<div class="navbar-header">
						<span class="nav-toggle-button collapse" data-target="#main-menu">
							<span class="sr-only">Navigation</span>
							<i class="fa fa-base"></i>
						</span>
					</div>
					<a class="navbar-brand" href="/root/index.php">Waterlogging</a>
					<div class="collapse navbar-collapse" id="main-menu">			
						<ul class="menu nav navbar-nav">
							<li class="nav-current" role="presentation">
								<a href="#introduction">简介<span class=""sr-only></span></a>
							</li>
							<li role="presentation"><a href="#city">城市内涝点</a></li>
							<li role="presentation"><a href="#daily">日内涝变化</a></li>
							<li role="presentation"><a href="#transfer">迁移学习</a></li>
							<li role="presentation"><a href="#statistics">内涝统计</a></li>
						</ul>
					</div>
				</div>
			</div>
		</div>
	</nav>
	<div class="main-body">
		<div class="container">
			<div class="jumbotron">
				<h1>城市内涝</h1>
				</br>
				<p>2012年北京7·21暴雨，立交桥被淹没，洪水、泥石流泛滥，78人死亡，190万人受灾</p>
				<p>2008年深圳6·13暴雨，8人死亡，6人失踪，转移受灾人口十多万人</p>
				<p>内涝究竟是什么，有多严重？</p>
				</br>
				<p><a class="btn btn-info btn-lg" href="#introduction" role="button">走进内涝数据</a></p>
			</div>
			<div class="blankfora" name="blankfora"><a name="introduction">anchor for introduction</a></div>
			<div class="introduction">
				<div class="page-header">
					<h1>简介</h1>
				</div>
				<p class="chinese">城市内涝,指由于强降水或连续性降水超过城市排水能力致使城市内产生积水灾害的现象。造成内涝的客观原因是降雨强度大，范围集中。降雨特别急的地方可能形成积水，降雨强度比较大、时间比较长也有可能形成积水。</p>
				<p class="chinese">从发生的区域来看，以前主要发生在一些沿海地势比较低的地区，内陆城市也经常发生。过去城市建设用地面积小，可选择的区域比较大，一般都选择地势比较高的地区建设，而现在城市用地十分紧张，可选择的余地少。</p>
				<p class="chinese">城市内涝的发生于区域密切相关，例如立交桥、地下通道等</p>
				<p class="chinese">总的来说，城市是否发生内涝是由基础设施和降水量所决定的。</p>
			</div>
			<div class="main-data">
				<div class="blankfora" name="blankfora"><a name="city">anchor for city</a></div>
				<div class="page-header">
					<h1>城市内涝点
						<small>
						<span class="dropdown citychoose">
							<button class="btn btn-default dropdown-toggle" type="button" id="dropdownMenu1" data-toggle="dropdown" aria-expanded="true"><span id="cityname">北京</span>
								<span class="caret"></span>
							</button>
							<ul class="dropdown-menu" role="menu" aria-labelledby="dropdownMenu1">
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">北京</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">杭州</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">石家庄</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">武汉</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">深圳</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">成都</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">上海</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">厦门</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">天津</a></li>
								<li role="presentation"><a role="menuitem" tabindex="-1" href="javascript:void(0);" onclick="changecity(this)">广州</a></li>
							</ul>
						</span>
							<input type="text" class="form-control" id="search">
							<button class="btn btn-default" id="searchButton">
								搜索
							</button>
							<button class="btn btn-primary" id="pointLayerOpen">
								麻点图
							</button>
							<button class="btn btn-danger" id="heatMapLayerOpen">
								热力图
							</button>
							<button class="btn btn-success" id="prepLayerOpen">
								降水图
							</button>
						</span>
						</small>
					</h1>
				</div>		
			</div>
		</div>
	</div>
	
	<div id="map_canvas">
	</div> 
	<div class="container">
		<div class="blankfora" name="blankfora"><a name="daily">anchor for daily</a></div>
		<div class="page-header">
			<h1>日内涝变化</h1>
		</div>
	</div>
	<iframe class="dailychange" width='70%' height='520' frameborder='0' src='http://shrineshine.cartodb.com/viz/6b4a1e3e-7f83-11e4-9c13-0e9d821ea90d/embed_map' allowfullscreen webkitallowfullscreen mozallowfullscreen oallowfullscreen msallowfullscreen>
	</iframe>

	<div class="container">
		<div class="blankfora" name="blankfora"><a name="transfer">anchor for transfer</a></div>
		<div class="page-header">
			<h1>迁移学习</h1>
		</div>
	</div>
	<div id="transferGraph">
		
	</div>
	<div class="container">
		<div class="blankfora" name="blankfora"><a name="statistics">anchor for statistics</a></div>
		<div class="page-header">
			<h1>内涝统计</h1>
		</div>
	</div>
	<div id='figure'>
	</div>
    <!-- jQuery (necessary for Bootstrap's JavaScript plugins) -->
    <script src="http://cdn.bootcss.com/jquery/1.11.2/jquery.min.js"></script>
    <!-- Include all compiled plugins (below), or include individual files as needed -->
    <script src="http://cdn.bootcss.com/bootstrap/3.3.4/js/bootstrap.min.js"></script>
	<script src="echarts-all.js"></script>
  </body>
</html>

<?php
	$con = mysql_connect("127.0.0.1","root","123456");
	$cn=0;
	if (!$con){
		die('Could not connect: ' . mysql_error());
	}
	mysql_select_db("waterlogging", $con);
	$pointAVG=mysql_query("SELECT AVG(count) FROM points");
	$result = mysql_query("SELECT * FROM points");
	echo "<script>var hotpoints=[";
	while($row = mysql_fetch_array($result)){
		$cn=$cn+1;
		echo '{"lng":'.$row['lng'].',"lat":'.$row['lat'].',"count":';
		switch($row['count']){
			case 1:
				echo "30";
				break;
			case 2:
				echo "40";
				break;
			case 3:
				echo "45";
				break;
		}
		if($cn!=685)
			echo "},";
		else
			echo "}";
	}
	echo "];</script>";

	$cn=0;
	$result = mysql_query("SELECT * FROM prep");
	echo "<script>var prep=[";
	while($row = mysql_fetch_array($result)){
		$cn=$cn+1;
		echo '{"lng":'.$row['lng'].',"lat":'.$row['lat'].',"count":'.floor($row["precipitation"]-44);
		if($cn!=685)
			echo "},";
		else
			echo "}";
	}
	echo "];</script>";
?>

<script type="text/javascript">
	$('.dropdown').hover(function() {
		$(this).find('.dropdown-menu').stop(true, true).delay(200).fadeIn(500);
	}, function() {
		$(this).find('.dropdown-menu').stop(true, true).delay(200).fadeOut(500);
	});
	
	function hasClass(ele,cls) {
		return ele.className.match(new RegExp('(\\s|^)'+cls+'(\\s|$)'));
	}
  
	function addClass(ele,cls) {
		if (!this.hasClass(ele,cls)) ele.className += " "+cls;
	}
  
	function removeClass(ele,cls) {
		if (hasClass(ele,cls)) {
			var reg = new RegExp('(\\s|^)'+cls+'(\\s|$)');
			ele.className=ele.className.replace(reg,' ');
		}
	}

	var menu=document.getElementById("main-menu");
	var mts=document.getElementsByName("blankfora");
	var lis=menu.getElementsByTagName("li");
	window.onscroll=function(){
		var ht=document.documentElement.scrollTop || document.body.scrollTop; 
		for(var i=0;i<mts.length;i++){
			if(i!=mts.length-1){
				if(mts.item(i).offsetTop<ht && mts.item(i+1).offsetTop>ht){
					addClass(lis.item(i),"nav-current");
				}
				else{
					removeClass(lis.item(i),"nav-current");
				}
			}
			else{
				if(mts.item(i).offsetTop<ht){
					addClass(lis.item(i),"nav-current");
				}
				else{
					removeClass(lis.item(i),"nav-current");
				}
			}
		}
	}


	var map = new BMap.Map("map_canvas");  //创建Map实例      
	var point = new BMap.Point(116.403694,39.927552);  //初始化地图，设置中心点
	map.centerAndZoom(point, 11);  //设置缩放级别           
	map.enableScrollWheelZoom();  //添加地图类型控件
	map.addControl(new BMap.NavigationControl());  //添加默认缩放平移控件
	
	var customLayer;
	function addCustomLayer(keyword) {
		if (customLayer) {
			map.removeTileLayer(customLayer);
		}
		customLayer=new BMap.CustomLayer({
			geotableId: 102504,
			q: '',
			tags: '', 
			filter: ''
		});
		map.addTileLayer(customLayer);
		customLayer.addEventListener('hotspotclick',callback);
	}
	function sevReturn(severity){
		switch(severity){
			case "1":return "严重";
			case "2":return "较为严重";
			case "3":return "极其严重";
		}
	}
	function callback(e)
	{
			var customPoi=e.customPoi;
			var contentPoi=e.content;
			
			var content='<p style="width:280px;margin:0;line-height:20px;">详细情况：'+customPoi.title+'<br/>日期：'+contentPoi.hz_date+'<br/>时间：'+contentPoi.hz_time+'<br/>严重程度：'+sevReturn(contentPoi.hz_type)+'<br/><img src='+contentPoi.weibo_pic+' width="240px" height="180px">'+'</p>';
			var searchInfoWindow = new BMapLib.SearchInfoWindow(map, content, {
				title: customPoi.address, 
				width: 290, 
				height: 280, 
				panel : "panel", 
				enableAutoPan : true,
				enableSendToPhone: true, 
				searchTypes :[
					BMAPLIB_TAB_SEARCH,   
					BMAPLIB_TAB_TO_HERE,  
					BMAPLIB_TAB_FROM_HERE
				]
			});
			var point = new BMap.Point(customPoi.point.lng, customPoi.point.lat);
			searchInfoWindow.open(point);
	}

	function addMarker(results,point,index){
		var marker=new BMap.Marker(point.point);
		var infoWindow=new BMap.InfoWindow("<div style='line-height:1.8em;font-size:12px;'><b>地址:</b>"+point.title+"</br><b>详细情况:</b>"+point.address+"</div>")
		marker.addEventListener("click",
			function(){
				marker.openInfoWindow(infoWindow);
			}
		);
		map.addOverlay(marker);
		map.centerAndZoom(point.point,11);
	}
	var searchComplete=function(results){   
		if (localSearch.getStatus()!=BMAP_STATUS_SUCCESS){
			return ;
		}
		for(var i=0;i<results.getCurrentNumPois();i++){
			var point=results.getPoi(i);
			addMarker(results,point,i);
		}
	}
	var localSearch = new BMap.LocalSearch(map,{onSearchComplete: searchComplete,renderOptions:{map: map}});
	document.getElementById("searchButton").onclick=function(){
		map.clearOverlays();
		map.centerAndZoom(point, 4);
		localSearch.search(document.getElementById("search").value,  {
			customData: {
				geotableId: 102504
			}
		});
	}
	
	addCustomLayer();
	var pointLayerFlag=1;
	document.getElementById("pointLayerOpen").onclick=function(){
		if(pointLayerFlag==1 && customLayer){
			map.removeTileLayer(customLayer);
			pointLayerFlag=0;
		}
		else{
			addCustomLayer();
			pointLayerFlag=1;
		}
	};

	$(document).ready(function(){
		$(".menu li").each(function(){
			$(this).click(function(){
				$(".menu .nav-current").removeClass("nav-current");
				$(this).addClass("nav-current");
			});
		});
	});

	function changecity(city){
		document.getElementById("cityname").innerHTML=city.innerHTML;
		switch(city.innerHTML){
			case "北京":map.centerAndZoom(new BMap.Point(116.403694,39.927552),11); break;
			case "杭州":map.centerAndZoom(new BMap.Point(120.20000,30.26667),11); break;
			case "石家庄":map.centerAndZoom(new BMap.Point(114.48333,38.03333),11); break;
			case "武汉":map.centerAndZoom(new BMap.Point(114.31667,30.51667),11); break;
			case "深圳":map.centerAndZoom(new BMap.Point(114.06667,22.61667),11); break;
			case "成都":map.centerAndZoom(new BMap.Point(104.06667,30.66667),11); break;
			case "厦门":map.centerAndZoom(new BMap.Point(118.10000,24.46667),11); break;
			case "上海":map.centerAndZoom(new BMap.Point(121.48,31.22),11); break;
			case "天津":map.centerAndZoom(new BMap.Point(117.20000,39.13333),11); break;
			case "广州":map.centerAndZoom(new BMap.Point(113.23333,23.16667),11); break;
		}
	}

	var transferGraph=echarts.init(document.getElementById('transferGraph'));
	var option = {
		backgroundColor: '#1b1b1b',
		color: ['gold','aqua','lime'],
		title : {
			text: '迁移学习',
			subtext:'',
			x:'center',
			textStyle : {
				color: '#fff'
			}
		},
		tooltip : {
			trigger: 'item',
			formatter: '{b}'
		},
		legend: {
			orient: 'vertical',
			x:'left',
			data:['北京', '杭州', '石家庄','武汉','深圳','成都','厦门','上海','天津','广州'],
			selectedMode: 'single',
			selected:{
				'杭州' : false,
				'石家庄' : false,
				'武汉' : false,
				'深圳' : false,
				'成都' : false,
				'厦门' : false,
				'上海' : false,
				'天津' : false,
				'广州' : false,
			},
			textStyle : {
				color: '#fff'
			}
		},
		toolbox: {
			show : true,
			orient : 'vertical',
			x: 'right',
			y: 'center',
			feature : {
				mark : {show: true},
				dataView : {show: true, readOnly: false},
				restore : {show: true},
				saveAsImage : {show: true}
			}
		},
		dataRange: {
			min : 0,
			max : 100,
			calculable : true,
			color: ['#ff3333', 'orange', 'yellow','lime','aqua'],
			textStyle:{
				color:'#fff'
			}
		},
		series : [
			{
				name: '全国',
				type: 'map',
				roam: true,
				hoverable: false,
				mapType: 'china',
				itemStyle:{
					normal:{
						borderColor:'rgba(100,149,237,1)',
						borderWidth:0.5,
						areaStyle:{
							color: '#1b1b1b'
						}
					}
				},
				data:[],
				markLine : {
					smooth:true,
					symbol: ['none', 'circle'],  
					symbolSize : 1,
					itemStyle : {
						normal: {
							color:'#fff',
							borderWidth:1,
							borderColor:'rgba(30,144,255,0.5)'
						}
					},
					data : [
						[{name:'上海'},{name:'北京'}],
						[{name:'上海'},{name:'厦门'}],
						[{name:'上海'},{name:'天津'}],
						[{name:'上海'},{name:'广州'}],
						[{name:'上海'},{name:'成都'}],
						[{name:'上海'},{name:'杭州'}],
						[{name:'上海'},{name:'武汉'}],
						[{name:'上海'},{name:'深圳'}],
						[{name:'上海'},{name:'石家庄'}],
						[{name:'北京'},{name:'厦门'}],
						[{name:'北京'},{name:'天津'}],
						[{name:'北京'},{name:'广州'}],
						[{name:'北京'},{name:'成都'}],
						[{name:'北京'},{name:'杭州'}],
						[{name:'北京'},{name:'武汉'}],
						[{name:'北京'},{name:'深圳'}],
						[{name:'北京'},{name:'石家庄'}],
						[{name:'厦门'},{name:'天津'}],
						[{name:'厦门'},{name:'广州'}],
						[{name:'厦门'},{name:'成都'}],
						[{name:'厦门'},{name:'杭州'}],
						[{name:'厦门'},{name:'武汉'}],
						[{name:'厦门'},{name:'深圳'}],
						[{name:'厦门'},{name:'石家庄'}],
						[{name:'天津'},{name:'广州'}],
						[{name:'天津'},{name:'成都'}],
						[{name:'天津'},{name:'杭州'}],
						[{name:'天津'},{name:'武汉'}],
						[{name:'天津'},{name:'深圳'}],
						[{name:'天津'},{name:'石家庄'}],
						[{name:'广州'},{name:'成都'}],
						[{name:'广州'},{name:'杭州'}],
						[{name:'广州'},{name:'武汉'}],
						[{name:'广州'},{name:'深圳'}],
						[{name:'广州'},{name:'石家庄'}],
						[{name:'成都'},{name:'杭州'}],
						[{name:'成都'},{name:'武汉'}],
						[{name:'成都'},{name:'深圳'}],
						[{name:'成都'},{name:'石家庄'}],
						[{name:'杭州'},{name:'武汉'}],
						[{name:'杭州'},{name:'深圳'}],
						[{name:'杭州'},{name:'石家庄'}],
						[{name:'武汉'},{name:'深圳'}],
						[{name:'武汉'},{name:'石家庄'}],
						[{name:'深圳'},{name:'石家庄'}],
					],
				},
				geoCoord: {
					'上海': [121.4648,31.2891],
					'北京': [116.4551,40.2539],                
					'厦门': [118.1689,24.6478],                
					'天津': [117.4219,39.4189],               
					'广州': [113.5107,23.2196],                
					'成都': [103.9526,30.7617],                
					'杭州': [119.5313,29.8773],                
					'武汉': [114.3896,30.6628],                
					'深圳': [114.5435,22.5439],             
					'石家庄': [114.4995,38.1006],   
				}
			},
			{
				name: '北京',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'上海',value:97},{name:'北京'}],
						[{name:'杭州',value:66},{name:'北京'}],
						[{name:'石家庄',value:75},{name:'北京'}],
						[{name:'厦门',value:55},{name:'北京'}],
						[{name:'天津',value:76},{name:'北京'}],
						[{name:'广州',value:85},{name:'北京'}],
						[{name:'成都',value:85},{name:'北京'}],
						[{name:'深圳',value:99},{name:'北京'}],
						[{name:'武汉',value:74},{name:'北京'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'北京',value:95},
					]
				}
			},
		    {
				name: '杭州',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'上海',value:97},{name:'杭州'}],
						[{name:'北京',value:95},{name:'杭州'}],
						[{name:'石家庄',value:75},{name:'杭州'}],
						[{name:'厦门',value:55},{name:'杭州'}],
						[{name:'天津',value:76},{name:'杭州'}],
						[{name:'广州',value:85},{name:'杭州'}],
						[{name:'成都',value:85},{name:'杭州'}],
						[{name:'深圳',value:99},{name:'杭州'}],
						[{name:'武汉',value:74},{name:'杭州'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'杭州',value:66},
					]
				}
			},
			{
				name: '上海',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'杭州',value:66},{name:'上海'}],
						[{name:'北京',value:95},{name:'上海'}],
						[{name:'石家庄',value:75},{name:'上海'}],
						[{name:'厦门',value:55},{name:'上海'}],
						[{name:'天津',value:76},{name:'上海'}],
						[{name:'广州',value:85},{name:'上海'}],
						[{name:'成都',value:85},{name:'上海'}],
						[{name:'深圳',value:99},{name:'上海'}],
						[{name:'武汉',value:74},{name:'上海'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'上海',value:97},
					]
				}
			},
			{
				name: '石家庄',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'杭州',value:66},{name:'石家庄'}],
						[{name:'北京',value:95},{name:'石家庄'}],
						[{name:'上海',value:97},{name:'石家庄'}],
						[{name:'厦门',value:55},{name:'石家庄'}],
						[{name:'天津',value:76},{name:'石家庄'}],
						[{name:'广州',value:85},{name:'石家庄'}],
						[{name:'成都',value:85},{name:'石家庄'}],
						[{name:'深圳',value:99},{name:'石家庄'}],
						[{name:'武汉',value:74},{name:'石家庄'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'石家庄',value:75},
					]
				}
			},
			{
				name: '厦门',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'杭州',value:66},{name:'厦门'}],
						[{name:'北京',value:95},{name:'厦门'}],
						[{name:'上海',value:97},{name:'厦门'}],
						[{name:'石家庄',value:75},{name:'厦门'}],
						[{name:'天津',value:76},{name:'厦门'}],
						[{name:'广州',value:85},{name:'厦门'}],
						[{name:'成都',value:85},{name:'厦门'}],
						[{name:'深圳',value:99},{name:'厦门'}],
						[{name:'武汉',value:74},{name:'厦门'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'厦门',value:55},
					]
				}
			},
			{
				name: '天津',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'杭州',value:66},{name:'天津'}],
						[{name:'北京',value:95},{name:'天津'}],
						[{name:'上海',value:97},{name:'天津'}],
						[{name:'石家庄',value:75},{name:'天津'}],
						[{name:'厦门',value:55},{name:'天津'}],
						[{name:'广州',value:85},{name:'天津'}],
						[{name:'成都',value:85},{name:'天津'}],
						[{name:'深圳',value:99},{name:'天津'}],
						[{name:'武汉',value:74},{name:'天津'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'天津',value:76},
					]
				}
			},
			{
				name: '广州',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'杭州',value:66},{name:'广州'}],
						[{name:'北京',value:95},{name:'广州'}],
						[{name:'上海',value:97},{name:'广州'}],
						[{name:'石家庄',value:75},{name:'广州'}],
						[{name:'厦门',value:55},{name:'广州'}],
						[{name:'天津',value:76},{name:'广州'}],
						[{name:'成都',value:85},{name:'广州'}],
						[{name:'深圳',value:99},{name:'广州'}],
						[{name:'武汉',value:74},{name:'广州'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'广州',value:85},
					]
				}
			},
			{
				name: '成都',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'杭州',value:66},{name:'成都'}],
						[{name:'北京',value:95},{name:'成都'}],
						[{name:'上海',value:97},{name:'成都'}],
						[{name:'石家庄',value:75},{name:'成都'}],
						[{name:'厦门',value:55},{name:'成都'}],
						[{name:'天津',value:76},{name:'成都'}],
						[{name:'广州',value:85},{name:'成都'}],
						[{name:'深圳',value:99},{name:'成都'}],
						[{name:'武汉',value:74},{name:'成都'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'成都',value:85},
					]
				}
			},
			{
				name: '深圳',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'杭州',value:66},{name:'深圳'}],
						[{name:'北京',value:95},{name:'深圳'}],
						[{name:'上海',value:97},{name:'深圳'}],
						[{name:'石家庄',value:75},{name:'深圳'}],
						[{name:'厦门',value:55},{name:'深圳'}],
						[{name:'天津',value:76},{name:'深圳'}],
						[{name:'广州',value:85},{name:'深圳'}],
						[{name:'成都',value:85},{name:'深圳'}],
						[{name:'武汉',value:74},{name:'深圳'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'深圳',value:99},
					]
				}
			},
			{
				name: '武汉',
				type: 'map',
				mapType: 'china',
				data:[],
				markLine : {
					smooth:true,
					effect : {
						show: true,
						scaleSize: 1,
						period: 30,
						color: '#fff',
						shadowBlur: 10
					},
					itemStyle : {
						normal: {
							borderWidth:1,
							lineStyle: {
								type: 'solid',
								shadowBlur: 10
							}
						}
					},
					data : [
						[{name:'杭州',value:66},{name:'武汉'}],
						[{name:'北京',value:95},{name:'武汉'}],
						[{name:'上海',value:97},{name:'武汉'}],
						[{name:'石家庄',value:75},{name:'武汉'}],
						[{name:'厦门',value:55},{name:'武汉'}],
						[{name:'天津',value:76},{name:'武汉'}],
						[{name:'广州',value:85},{name:'武汉'}],
						[{name:'成都',value:85},{name:'武汉'}],
						[{name:'深圳',value:99},{name:'武汉'}],
					]
				},
				markPoint : {
					symbol:'emptyCircle',
					symbolSize : function (v){
					    return 10 + v/10
					},
					effect : {
						show: true,
						shadowBlur : 0
					},
					itemStyle:{
						normal:{
							label:{show:false}
						},
						emphasis: {
							label:{position:'top'}
						}
					},
					data : [
						{name:'武汉',value:74},
					]
				}
			}
		]
	};
	transferGraph.setOption(option);
	if(!isSupportCanvas()){
    	alert('热力图目前只支持有canvas支持的浏览器,您所使用的浏览器不能使用热力图功能~');
    };

	heatmapOverlay = new BMapLib.HeatmapOverlay({"radius":20});
	heatmapOverlayForPrep = new BMapLib.HeatmapOverlay({"radius":20});
	map.addOverlay(heatmapOverlay);
	map.addOverlay(heatmapOverlayForPrep);
	heatmapOverlay.setDataSet({data:hotpoints,max:100});
	heatmapOverlayForPrep.setDataSet({data:prep,max:200});
	heatmapOverlay.hide();
	heatmapOverlayForPrep.hide();
	var heatMapLayerFlag=0;
	var heatMapLayerForPrepFlag=0;
	
	function isSupportCanvas(){
        var elem = document.createElement('canvas');
        return !!(elem.getContext && elem.getContext('2d'));
    };
	document.getElementById("heatMapLayerOpen").onclick=function(){
		if(heatMapLayerFlag==0){
			if(heatMapLayerForPrepFlag==1){
				heatmapOverlayForPrep.hide();
				heatMapLayerForPrepFlag=0;
			}
			heatmapOverlay.show();
			heatMapLayerFlag=1;
		}
		else{
			heatmapOverlay.hide();
			heatMapLayerFlag=0;
		}
	};
	document.getElementById("prepLayerOpen").onclick=function(){
		if(heatMapLayerForPrepFlag==0){
			if(heatMapLayerFlag==1){
				heatmapOverlay.hide();
				heatMapLayerFlag=0;
			}
			heatmapOverlayForPrep.show();
			heatMapLayerForPrepFlag=1;
		}
		else{
			heatmapOverlayForPrep.hide();
			heatMapLayerForPrepFlag=0;
		}
	};
	

</script>
<script>
function showFigure(id,cityData){
	//定义柱状图所在svg的宽和高
	var histoSVGwidth=600;
	var histoSVGheight=400;
	var barColor='#44A3BB';
	function segColor(c){
		return {
			low:"#807dba",
			mid:"#e08214",
			high:"#C43457"
		}[c]; 
	}
	//为饼状图准备数据，每一种内涝程度对应的数量
	var dataForPie=['low','mid','high'].map(function(d){ 
        return {
			severity:d,
			freq:d3.sum(cityData.map(function(c){return c.freq[d];}))
		}; 
	}); //tf
	//为柱状图准备数据，每一个城市内涝的总数量
	cityData.forEach(
		function(d){
			d.total=d.freq.low+d.freq.mid+d.freq.high;
		}
	);
	var dataForHisto=cityData.map(
		function(d){
			return [d.name,d.total];
		}
	);//sf
	var histoData=histogram(dataForHisto); 
    var pieData=pieChart(dataForPie);
    var frameData=frameChart(dataForPie); 
    
    function histogram(dataForHisto){
        var histoData={};
		//柱状图四个方向的padding
		var histoFrame={
			top:70,
			right:0,
			bottom:30,
			left:0
		};
		var histoWidth=histoSVGwidth-histoFrame.left-histoFrame.right;
		var histoHeight=histoSVGheight-histoFrame.top-histoFrame.bottom;
		//建立svg并位移出设定的padding
        var histoSVG=d3.select(id)
			.append("svg")
            .attr("width",histoSVGwidth)
            .attr("height",histoSVGheight)
			.append("g")
            .attr("transform","translate("+histoFrame.left+","+histoFrame.top+")");
		//建立x轴的离散型Scale，每一个cityname用rangeRoundBands绑定距离
        var xAxis=d3.scale
			.ordinal()
			.domain(
				dataForHisto.map(
					function(d){
						return d[0];
					}
				)
			)
			.rangeRoundBands([0,histoWidth], 0.1);
        //在svg中加入x轴
        histoSVG.append("g")
			.attr("class","x axis")
            .attr("transform","translate(0,"+histoHeight+")")
            .call(d3.svg
				.axis()
				.scale(xAxis)
				.orient("bottom")
			);
		//建立y轴的线性Scale
        var yAxis=d3.scale
			.linear()
			.domain([0,d3.max(dataForHisto,function(d){
				return d[1];
			})])
			.range([histoHeight,0]);
		//柱状图
        var bars=histoSVG.selectAll(".bar")
			.data(dataForHisto)
			.enter()
            .append("g")
			.attr("class","bar");
        bars.append("rect")
            .attr("x", function(d){return xAxis(d[0]);})
            .attr("y", function(d){return yAxis(d[1]);})
            .attr("width", xAxis.rangeBand())
            .attr("height", function(d){
				return histoHeight-yAxis(d[1]);
			})
            .attr('fill',barColor)
            .on("mouseover",mouseover)
            .on("mouseout",mouseout);
        bars.append("text")
			.text(function(d){ 
				return d3.format(",")(d[1])
			})
            .attr("x", function(d){return xAxis(d[0])+xAxis.rangeBand()/2; })
            .attr("y", function(d){return yAxis(d[1])-8;})
            .attr("text-anchor", "middle");
        
        function mouseover(d){ 
            var cityChoosed=cityData.filter(function(c){return c.name==d[0];})[0];
            var cityDataForPie=d3.keys(cityChoosed.freq).map(function(c){return {severity:c,freq:cityChoosed.freq[c]};});
			histoData.keep(cityData.map(function(c){
				if (c.name==d[0]){
					return [c.name,c.total];
				}
				else{
					return [c.name,0];
				}
			}),barColor);
            pieData.update(cityDataForPie);
            frameData.update(cityDataForPie);
        }
        
        function mouseout(d){     
			histoData.update(cityData.map(function(c){
                return [c.name,c.total];}),barColor);
            pieData.update(dataForPie);
            frameData.update(dataForPie);
        }
        
        histoData.update=function(newData,color){
            yAxis.domain([0,d3.max(newData,function(d){return d[1];})]);
            var bars=histoSVG.selectAll(".bar").data(newData);
            bars.select("rect")
				.transition()
				.duration(600)
                .attr("y", function(d){return yAxis(d[1]);})
                .attr("height", function(d){return histoHeight-yAxis(d[1]);})
                .attr("fill",color);
            bars.select("text")
				.transition()
				.duration(600)
                .text(function(d){return d3.format(",")(d[1])})
                .attr("y",function(d){return yAxis(d[1])-8;});            
        }
		histoData.keep=function(newData,color){
            var bars=histoSVG.selectAll(".bar").data(newData);
            bars.select("rect")
				.transition()
				.duration(600)
                .attr("y", function(d){return yAxis(d[1]);})
                .attr("height", function(d){return histoHeight-yAxis(d[1]);})
                .attr("fill",color);
            bars.select("text")
				.transition()
				.duration(600)
                .text(function(d){return d3.format(",")(d[1])})
                .attr("y",function(d){return yAxis(d[1])-8;});            
        }   
        return histoData;
    }

    function pieChart(dataForPie){
        var pieData={};
		var pieWidth=300;
		var pieHeight=300;
        var pieRadius=Math.min(pieWidth,pieHeight)/2;
        var pieSVG=d3.select(id)
			.append("svg")
            .attr("width",pieWidth)
			.attr("height",pieHeight)
			.append("g")
            .attr("transform", "translate("+pieRadius+","+pieRadius+")");
        var arc=d3.svg
			.arc()
			.outerRadius(pieRadius-10)
			.innerRadius(0);
        var pie=d3.layout
			.pie()
			.sort(null)
			.value(function(d){return d.freq;});
        pieSVG.selectAll("path")
			.data(pie(dataForPie))
			.enter()
			.append("path")
			.attr("d",arc)
            .each(function(d){this._current=d;})
            .style("fill", function(d){return segColor(d.data.severity);})
            .on("mouseover",mouseover)
			.on("mouseout",mouseout);
        pieData.update=function(newData){
            pieSVG.selectAll("path")
				.data(pie(newData))
				.transition()
				.duration(600)
                .attrTween("d",arcTween);
        }       
		function arcTween(d){
            var i=d3.interpolate(this._current,d);
            this._current=i(0);
            return function(c){return arc(i(c));};
        }    
        function mouseover(d){
            histoData.update(cityData.map(function(c){ 
                return [c.name,c.freq[d.data.severity]];
			})
			,segColor(d.data.severity));
        }
        function mouseout(d){
            histoData.update(cityData.map(function(c){
                return [c.name,c.total];}),barColor);
        }
    return pieData;
    }
    
    function frameChart(dataForPie){
        var frameData={};
        var table=d3.select(id)
			.append("table")
			.attr('class','frameChart');
        var tr=table.append("tbody")
			.selectAll("tr")
			.data(dataForPie)
			.enter()
			.append("tr");
        tr.append("td")
			.append("svg")
			.attr("width",'16')
			.attr("height",'16')
			.append("rect")
            .attr("width", '16')
			.attr("height", '16')
			.attr("fill",function(d){return segColor(d.severity);});
        tr.append("td")
			.text(function(d){return d.severity;});
        tr.append("td")
			.attr("class",'frameFreq')
            .text(function(d){return d3.format(",")(d.freq);});
        tr.append("td")
			.attr("class",'framePerc')
            .text(function(d){return getLegend(d,dataForPie);});
        frameData.update=function(newData){
            var l=table.select("tbody")
				.selectAll("tr")
				.data(newData);
            l.select(".frameFreq")
				.text(function(d){return d3.format(",")(d.freq);});
            l.select(".framePerc")
				.text(function(d){return getLegend(d,newData);});        
        }
        function getLegend(d,dataForPie){ 
            return d3.format("%")(d.freq/d3.sum(dataForPie.map(function(c){return c.freq;})));
        }

        return frameData;
    }
    


}
</script>

<script>
var cityData=[
	{name:'北京',freq:{low:134, mid:14, high:2}},
	{name:'天津',freq:{low:96, mid:8, high:2}},
	{name:'石家庄',freq:{low:72, mid:4, high:2}},
	{name:'上海',freq:{low:52, mid:6, high:2}},
	{name:'成都',freq:{low:9, mid:1, high:0}},
	{name:'武汉',freq:{low:19, mid:1, high:0}},
	{name:'杭州',freq:{low:95, mid:18, high:9}},
	{name:'广州',freq:{low:69, mid:8, high:1}},
	{name:'深圳',freq:{low:51, mid:3, high:1}},
	{name:'厦门',freq:{low:6, mid:0, high:0}}
];

showFigure('#figure',cityData);
</script>
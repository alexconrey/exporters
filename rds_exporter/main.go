package main

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/cloudwatch"
    "github.com/aws/aws-sdk-go/service/rds"
    "flag"
    "fmt"
    "log"
    "time"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"strings"
)

var (
  namespace = string("AWS/RDS")
  dimension = string("DBInstanceIdentifier")
)

var instances []RDSInstance

var metric_types []string

type RDSInstance struct {
	Name string
	Metrics []RDSMetric
}

type RDSMetric struct {
    Name string
    Value float64
}

var gauges []PrometheusGauge

type PrometheusGauge struct {
	Name string
	Gauge *prometheus.GaugeVec
}

func getInstancesForRegion(region string) []RDSInstance {
	svc := rds.New(session.New(&aws.Config{
		Region: aws.String(region),
	}))

	input := &rds.DescribeDBInstancesInput{}

	result, err := svc.DescribeDBInstances(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeDBInstanceNotFoundFault:
				fmt.Println(rds.ErrCodeDBInstanceNotFoundFault, aerr.Error())
        	default:
          	  fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		log.Println(err)
	}

	var instances []RDSInstance

	for _, instance := range result.DBInstances {
		ins := RDSInstance{
			Name: *instance.DBInstanceIdentifier,
		}
		instances = append(instances, ins)
	}

	return instances
}

func getMetricsForInstance(instance string, region string) []string {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String(region)},
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := cloudwatch.New(sess)

	getMet := make(chan bool)

	var result *cloudwatch.ListMetricsOutput

	go func() {
		res, err := svc.ListMetrics(&cloudwatch.ListMetricsInput{
            Namespace: aws.String(namespace),
            Dimensions: []*cloudwatch.DimensionFilter{
            	&cloudwatch.DimensionFilter{
            		Name: aws.String(dimension),
            		Value: aws.String(instance),
            	},
            },
		})

		if err != nil {
			log.Fatal(err)
		}

		result = res

		getMet <- true
	}()

	<-getMet

	var metrics []string

	for _, res := range result.Metrics {
		// If metric is globally known, don't redeclare and also
		// don't create a duplicate GaugeVec
		KNOWN_METRIC := false
		for _, metric_type := range metric_types {
			if metric_type == *res.MetricName {
				KNOWN_METRIC = true
			}
		}
		if ! KNOWN_METRIC {
			metric_types = append(metric_types, *res.MetricName)
			pg := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: fmt.Sprintf("rds_instance_%s", *res.MetricName),
					Help: fmt.Sprintf("help %s", *res.MetricName),
				},
				gauge_fields,
			)
			gauge := PrometheusGauge{
				Name: *res.MetricName,
				Gauge: pg,
			}
			gauges = append(gauges, gauge)
		}
		metrics = append(metrics, *res.MetricName)
	}

	return metrics

}

func getValueForMetric(metric string, instance string, stat string) float64 {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := cloudwatch.New(sess)
	// CloudWatch static polling set to 120s (2min) period
	endTime := time.Now()
	duration, _ := time.ParseDuration("-2m")
	startTime := endTime.Add(duration)
	metricId := "m1"
	period := int64(120)

	result, err := svc.GetMetricData(&cloudwatch.GetMetricDataInput{
		StartTime: &startTime, // 2 minutes ago
		EndTime: &endTime, // Now
		MetricDataQueries: []*cloudwatch.MetricDataQuery{
			&cloudwatch.MetricDataQuery{
				Id: &metricId,
				MetricStat: &cloudwatch.MetricStat{
					Metric: &cloudwatch.Metric{
						Namespace: aws.String(namespace),
						MetricName: &metric,
						Dimensions: []*cloudwatch.Dimension{
							&cloudwatch.Dimension{
								Name: aws.String(dimension),
								Value: aws.String(instance),
							},
						},
					},
					Period: &period,
					Stat: &stat,
				},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	results := result.MetricDataResults
	for _, result := range results {
		for _, value := range result.Values {
			return *value
		}
	}
	if *debug {
		////log.Println("No result found for metric", metric, "on instance", instance)
	}

	return -1.0
}

var (
    gauge_fields = []string {
    	"instance",
    	"region_name",
    	"statistic",
    }

    statistics = []string {
    	"Average",
 //   	"Minimum",
 //   	"Maximum",
    }
)


// Define flags here
var (
	listen_addr = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	aws_regions = flag.String("regions", "us-east-1,us-east-2,ca-central-1", "List of regions")
	interval = flag.Int("interval", 30, "The interval (in seconds) to wait between stats queries")
	debug = flag.Bool("debug", false, "Set this to enable debug mode")
)

func getMetrics(regions []string, stats []string) {
    for _, region := range regions {
    	for _, instance := range getInstancesForRegion(region) {
    		for _, metric := range getMetricsForInstance(instance.Name, region) {
    			for _, statistic := range stats {
    				metric_val := getValueForMetric(metric, instance.Name, statistic)
    				// Retry if metric_val is -1 (error)
    				if metric_val == -1.0 {

    					if *debug {
    						////log.Println("Retrying metric", metric)
    					}
    					metric_val = getValueForMetric(metric, instance.Name, statistic)
    				}
	    			// Iterate thru gauges until we find one matching this metric
    				for _, gauge := range gauges {
    					gauge.Gauge.With(prometheus.Labels{
    						"instance": instance.Name,
    						"region_name": region,
    						"statistic": statistic,
    					}).Set(metric_val)
    				}
    			}
    		}
    	}
    }
}

func init() {

}

var regions []string

func main() {
	log.Println("Starting...")
	flag.Parse()
	regions := strings.Split(*aws_regions, ",")

	// Run getMetricsForInstance on first available instance only to initialize gauges
	oneRegion := []string{
		regions[0],
	}
	oneInstance := getInstancesForRegion(oneRegion[0])[0].Name

	getMetricsForInstance(oneInstance, oneRegion[0])
	log.Println("Started AWS/RDS Exporter")

	// Register newly created gauges outside of the function after all have been created
	for _, gauge := range gauges {
		prometheus.MustRegister(gauge.Gauge)
	}

	// This is set once the data is fully initialized (after getMetrics runs)
	IS_FULL_INIT := false

	// Infinite loop running getMetrics $interval seconds 
	go func() {
		for {
			runStart := time.Now()
			getMetrics(regions, statistics)
			if ! IS_FULL_INIT {
				log.Println("Initial data population complete")
				IS_FULL_INIT = true
			}
			exec_time := int64(time.Since(runStart).Seconds())
			if *debug {
				log.Println("Full run (sec):", exec_time)
			}
			// Factor execution time in sleep delay (4min total vs 4min + exec time)
			if (int64(*interval) > exec_time) {
				wait_time := int64(*interval) - exec_time
				if *debug {
					log.Println("  Wait (exec + wait = interval):", wait_time)
				}
				time.Sleep(time.Duration(wait_time))
			} 
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*listen_addr, nil))
}
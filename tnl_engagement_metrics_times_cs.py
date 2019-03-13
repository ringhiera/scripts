# coding: utf-8

# In`17`:

from google.cloud import bigquery
from datetime import datetime
import os
import uuid
import time
import yaml
import sys



DIG_USAGE = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12weeks_usage',
    'sql_query': """
        SELECT
            customer_id as userid,
            cast(CURRENT_DATE() as date)  as period_current,
            date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY) AS period_active, 
            DATE_DIFF(date_add(cast(date_TRUNC(cast(Current_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY),date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY), DAY) as roll_cont_weeks,
            case when lower(product) LIKE  '%iphone app%' or lower(product)  LIKE  '%android app%' or lower(product) LIKE  '%ios app%' then 'Mobile'
            when lower(product)  LIKE  '%ipad%' or lower(product)  LIKE  '%android tablet%' or lower(product)  LIKE  '%kindle%' then 'Tablet'
            when lower(product)  in ('') then 'Unknown' ELSE 'Desktop' END Device,
            case when lower(product)  LIKE  '%iphone%' then 'IPhone'
            when lower(product)  LIKE  '%android%' then 'Android'
            when lower(product)  LIKE  '%ios app%' then 'IPhone'
            when lower(product)  LIKE  '%ipad%' then 'IPad'
            when lower(product)  LIKE  '%android tablet%' then 'Android'
            when lower(product)  LIKE  '%kindle%' then 'Kindle'
            when lower(product)  in ('') then 'Unknown' ELSE 'Website' END Device_Type,
            'Times Network Digital Products - Past 12 weeks Usage' AS Product_usage_duration,
            product  AS Product,
            DATE_DIFF((cast(DATE_ADD(CURRENT_DATE(),interval 0 DAY) as date)), (max(cast(activity_date as date))), DAY) as Recency,
            count(distinct(cast(activity_date as date))) AS active_days,
            count(distinct(cast(WEEKEND as date))) AS weekend,
            count(distinct(cast(WEEKDAY as date))) AS weekdays,
            COUNT(*) events,
            count(distinct(session_id)) AS sessions,
            SUM(CASE WHEN event_type = '0' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = '0' AND previous_hit_page_name = 'search results' THEN 1 ELSE 0 END) AS views_from_search, #WEB ONLY
            SUM(is_comment) AS comments,
            SUM(is_share) AS shares, #224 = START, 226 = BUTTON
            SUM(is_save_add) AS favourite_adds,
            SUM(is_save_remove) AS favourite_removes,
            SUM(is_video_play) AS video_starts,
            SUM(is_video_complete) AS video_completes
            FROM(Select *,(case when FORMAT_DATE('%A', cast(activity_date as date)) in ('Saturday','Sunday') then activity_date  else null end) AS WEEKEND,
            (case when FORMAT_DATE('%A', cast(activity_date as date)) not in ('Saturday','Sunday') then activity_date else null end) AS WEEKDAY  
            FROM `newsuk-datatech-prod.inca_clickstream_tables.times_clickstream_daily`
            WHERE timestamp(activity_date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as TIMESTAMP)  AND timestamp(activity_date)  <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as TIMESTAMP) and
            cast(activity_date as date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as date) AND cast(activity_date as date) <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as date)
            and customer_id is not null AND product IS NOT NULL and
            lower(product) in ("t&st android app","t&st android app - irish app","t&st android app beta","t&st iphone app","t&st iphone app - irish app","t&st iphone app - times of london app",
            "the sunday times","the sunday times puzzles","the times","the times - ireland","the times acquisition store","the times acquisition store - ireland","the times acquisition store - uk",
            "the times and sunday times","the times and sunday times android app","the times and sunday times android app - irish app","the times and sunday times android tablet app",
            "the times and sunday times android tablet app - irish app","the times and sunday times ipad app","the times and sunday times ipad app - irish app","the times and sunday times iphone app",
            "the times and sunday times kindle app","the times city guides site","the times help hub site","the times ireland membership site","the times politics site","the times sport android app",
            "the times sport ios app","times puzzles","times+","the times and sunday times iphone app - irish app")) 
            #REGEXP_REPLACE(post_prop11, 'CPN:', '') = 'AAAA003110137'
            GROUP BY userid,period_current,period_active,roll_cont_weeks,Product_usage_duration,Product,Device,Device_Type
        """
}
#saved as `tnl_12weeks_usage`


DIG_USAGE_ALL = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12weeks_usage_all',
    'sql_query': """
        SELECT
            customer_id as userid,
            cast(CURRENT_DATE() as date)  as period_current,
            date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY) AS period_active,
            DATE_DIFF(date_add(cast(date_TRUNC(cast(Current_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY),date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY), DAY) as roll_cont_weeks,
            'All' as Device,
            'All' as Device_Type,
            'Times Network Digital Products - Past 12 weeks Usage' AS Product_usage_duration,
            'All' as Product,
            DATE_DIFF((cast(DATE_ADD(CURRENT_DATE(),interval 0 DAY) as date)), (max(cast(activity_date as date))), DAY) as Recency,
            count(distinct(cast(activity_date as date))) AS active_days,
            count(distinct(cast(WEEKEND as date))) AS weekend,
            count(distinct(cast(WEEKDAY as date))) AS weekdays,
            COUNT(*) events,
            count(distinct(session_id)) AS sessions,
            SUM(CASE WHEN event_type = '0' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = '0' AND previous_hit_page_name = 'search results' THEN 1 ELSE 0 END) AS views_from_search, #WEB ONLY
            SUM(is_comment) AS comments,
            SUM(is_share) AS shares, #224 = START, 226 = BUTTON
            SUM(is_save_add) AS favourite_adds,
            SUM(is_save_remove) AS favourite_removes,
            SUM(is_video_play) AS video_starts,
            SUM(is_video_complete) AS video_completes
            FROM(Select *,(case when FORMAT_DATE('%A', cast(activity_date as date)) in ('Saturday','Sunday') then activity_date  else null end) AS WEEKEND,
            (case when FORMAT_DATE('%A', cast(activity_date as date)) not in ('Saturday','Sunday') then activity_date else null end) AS WEEKDAY  
            FROM `newsuk-datatech-prod.inca_clickstream_tables.times_clickstream_daily`
            WHERE timestamp(activity_date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as TIMESTAMP)  AND timestamp(activity_date)  <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as TIMESTAMP) and
            cast(activity_date as date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as date) AND cast(activity_date as date) <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as date)
            and customer_id is not null AND product IS NOT NULL and
            lower(product) in ("t&st android app","t&st android app - irish app","t&st android app beta","t&st iphone app","t&st iphone app - irish app","t&st iphone app - times of london app",
            "the sunday times","the sunday times puzzles","the times","the times - ireland","the times acquisition store","the times acquisition store - ireland","the times acquisition store - uk",
            "the times and sunday times","the times and sunday times android app","the times and sunday times android app - irish app","the times and sunday times android tablet app",
            "the times and sunday times android tablet app - irish app","the times and sunday times ipad app","the times and sunday times ipad app - irish app","the times and sunday times iphone app",
            "the times and sunday times kindle app","the times city guides site","the times help hub site","the times ireland membership site","the times politics site","the times sport android app",
            "the times sport ios app","times puzzles","times+","the times and sunday times iphone app - irish app"))  
            #REGEXP_REPLACE(post_prop11, 'CPN:', '') = 'AAAA003110137'
            GROUP BY userid,period_current,period_active,roll_cont_weeks,Product_usage_duration,Product,Device,Device_Type
        """
}
#saved as `tnl_12weeks_usage_all`

DIG_DWELL = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12weeks_dwell',
    'sql_query': """
        SELECT
            customer_id as userid,
            cast(CURRENT_DATE() as date)  as period_current,
            date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY) AS period_active, 
            DATE_DIFF(date_add(cast(date_TRUNC(cast(Current_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY),date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY), DAY) as roll_cont_weeks,
            case when lower(product) LIKE  '%iphone app%' or lower(product)  LIKE  '%android app%' or lower(product) LIKE  '%ios app%' then 'Mobile'
            when lower(product)  LIKE  '%ipad%' or lower(product)  LIKE  '%android tablet%' or lower(product)  LIKE  '%kindle%' then 'Tablet'
            when lower(product)  in ('') then 'Unknown' ELSE 'Desktop' END Device,
            case when lower(product)  LIKE  '%iphone%' then 'IPhone'
            when lower(product)  LIKE  '%android%' then 'Android'
            when lower(product)  LIKE  '%ios app%' then 'IPhone'
            when lower(product)  LIKE  '%ipad%' then 'IPad'
            when lower(product)  LIKE  '%android tablet%' then 'Android'
            when lower(product)  LIKE  '%kindle%' then 'Kindle'
            when lower(product)  in ('') then 'Unknown' ELSE 'Website' END Device_Type,
            'Times Network Digital Products - Past 12 weeks Usage' AS Product_usage_duration,
            product  AS Product,
            ifnull(round(SUM(time_spent_secs/60)),0) AS DWELL_MINS
            FROM `newsuk-datatech-prod.inca_clickstream_tables.times_clickstream_daily`
            WHERE timestamp(activity_date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as TIMESTAMP)  AND timestamp(activity_date)  <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as TIMESTAMP) and
            cast(activity_date as date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as date) AND cast(activity_date as date) <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as date)
            and customer_id is not null AND product IS NOT NULL and
            lower(product) in ("t&st android app","t&st android app - irish app","t&st android app beta","t&st iphone app","t&st iphone app - irish app","t&st iphone app - times of london app",
            "the sunday times","the sunday times puzzles","the times","the times - ireland","the times acquisition store","the times acquisition store - ireland","the times acquisition store - uk",
            "the times and sunday times","the times and sunday times android app","the times and sunday times android app - irish app","the times and sunday times android tablet app",
            "the times and sunday times android tablet app - irish app","the times and sunday times ipad app","the times and sunday times ipad app - irish app","the times and sunday times iphone app",
            "the times and sunday times kindle app","the times city guides site","the times help hub site","the times ireland membership site","the times politics site","the times sport android app",
            "the times sport ios app","times puzzles","times+","the times and sunday times iphone app - irish app") and time_spent_secs > 0  
            GROUP BY userid,period_current,period_active,roll_cont_weeks,Product_usage_duration,Product,Device,Device_Type
        """
}
#saved as `tnl_12weeks_dwell`

DIG_DWELL_ALL = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12weeks_dwell_all',
    'sql_query': """              
        SELECT
        customer_id as userid,
        cast(CURRENT_DATE() as date)  as period_current,
        date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY) AS period_active, 
        DATE_DIFF(date_add(cast(date_TRUNC(cast(Current_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY),date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY), DAY) as roll_cont_weeks,
        'All' as product,
        'All' as Device,
        'All' as Device_Type,
        'Times Network Digital Products - Past 12 weeks Usage' AS Product_usage_duration,        
        ifnull(round(SUM(time_spent_secs/60)),0) AS DWELL_MINS
        FROM `newsuk-datatech-prod.inca_clickstream_tables.times_clickstream_daily`
        WHERE timestamp(activity_date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as TIMESTAMP)  AND timestamp(activity_date)  <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as TIMESTAMP) and
        cast(activity_date as date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as date) AND cast(activity_date as date) <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as date)
        and customer_id is not null AND product IS NOT NULL and
        lower(product) in ("t&st android app","t&st android app - irish app","t&st android app beta","t&st iphone app","t&st iphone app - irish app","t&st iphone app - times of london app",
        "the sunday times","the sunday times puzzles","the times","the times - ireland","the times acquisition store","the times acquisition store - ireland","the times acquisition store - uk",
        "the times and sunday times","the times and sunday times android app","the times and sunday times android app - irish app","the times and sunday times android tablet app",
        "the times and sunday times android tablet app - irish app","the times and sunday times ipad app","the times and sunday times ipad app - irish app","the times and sunday times iphone app",
        "the times and sunday times kindle app","the times city guides site","the times help hub site","the times ireland membership site","the times politics site","the times sport android app",
        "the times sport ios app","times puzzles","times+","the times and sunday times iphone app - irish app") and time_spent_secs > 0 
        GROUP BY userid,period_current,period_active,roll_cont_weeks,Product_usage_duration,Product,Device,Device_Type
        """
}

#join together to create device+all view

def save_query1(project):
    query_list1 = {}    
    DIG_USAGE_OVERALL = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12weeks_usage_overall',
    'sql_query': """
        select * from
        (select * from `{project_name}.tnl_engagement_metrics.tnl_12weeks_usage`) UNION ALL
        (select * from `{project_name}.tnl_engagement_metrics.tnl_12weeks_usage_all`)
        """.format(project_name=project)
}
#saved as `tnl_12weeks_usage_overall`
#saved as `tnl_12weeks_dwell_all`
    DIG_DWELL_OVERALL = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12weeks_dwell_overall',
    'sql_query': """
        select * from
        (select * from `{project_name}.tnl_engagement_metrics.tnl_12weeks_dwell`) UNION ALL
        (select * from `{project_name}.tnl_engagement_metrics.tnl_12weeks_dwell_all`)
        """.format(project_name=project)
}
#saved as `tnl_12weeks_dwell_overall`
#merge dwell and other metrics to create master dataset:inner join
    DIG_USAGE_MASTER = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12weeks_dwell_master_IJ',
    'sql_query': """
        Select met.*,dt.dwell_mins from `{project_name}.tnl_engagement_metrics.tnl_12weeks_usage_overall` met
        LEFT join `{project_name}.tnl_engagement_metrics.tnl_12weeks_dwell_overall` dt
        on met.userid=dt.userid
        and met.period_current=dt.period_current
        and met.period_active=dt.period_active
        and met.product=dt.product
        """.format(project_name=project)
}
#saved as `tnl_12weeks_dwell_master_IJ` will be using this
#create week/2weeks/4weeks/12weeks views with high/med/low engagement for each
#1week usage
    DIG_1WEEK_USAGE = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_1week_agg',
    'sql_query': """
        SELECT userid,period_current,Device,Device_Type,Product,min(Recency) AS recency,
        sum(active_days) as active_days,
        sum(weekend) as weekend,
        ROUND(sum(weekend)/2,2)*100 as weekend_pct,
        sum(weekdays) as weekday,
        ROUND(sum(weekdays)/5, 2)*100 as weekday_pct,
        sum(events) as events,
        sum(sessions) as sessions,
        sum(views) as views,
        sum(views_from_search) as views_from_search, 
        sum(comments) as comments,
        sum(shares) as shares,
        sum(favourite_adds) as favourite_adds,
        sum(favourite_removes) as favourite_removes,
        sum(video_starts) as video_starts,
        sum(video_completes) AS video_completes,
        sum(dwell_mins) AS dwell_mins,
        'Recent 1 week usage' as Product_usage_duration
        FROM `{project_name}.tnl_engagement_metrics.tnl_12weeks_dwell_master_IJ` 
        where roll_cont_weeks in(14)
        group by userid,period_current,Device,Device_Type,Product
        """.format(project_name=project)
}
#saved as`{project_name}:tnl_engagement_metrics.tnl_1week_agg`
    DIG_2WEEK_USAGE = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_2week_agg',
    'sql_query': """
        SELECT userid,period_current,Device,Device_Type,Product,min(Recency) AS recency,
        sum(active_days) as active_days,
        sum(weekend) as weekend,
        ROUND(sum(weekend)/4,2)*100 as weekend_pct,
        sum(weekdays) as weekday,
        ROUND(sum(weekdays)/10, 2)*100 as weekday_pct,
        sum(events) as events,
        sum(sessions) as sessions,
        sum(views) as views,
        sum(views_from_search) as views_from_search, 
        sum(comments) as comments,
        sum(shares) as shares,
        sum(favourite_adds) as favourite_adds,
        sum(favourite_removes) as favourite_removes,
        sum(video_starts) as video_starts,
        sum(video_completes) AS video_completes,
        sum(dwell_mins) AS dwell_mins,
        'Recent 2 week usage' as Product_usage_duration
        FROM `{project_name}.tnl_engagement_metrics.tnl_12weeks_dwell_master_IJ` 
        where roll_cont_weeks in(14,21)
        group by userid,period_current,Device,Device_Type,Product
        """.format(project_name=project)
}
#saved as`{project_name}:tnl_engagement_metrics.tnl_2week_agg`
    DIG_4WEEK_USAGE = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_4week_agg',
    'sql_query': """
        SELECT userid,period_current,Device,Device_Type,Product,min(Recency) AS recency,
        sum(active_days) as active_days,
        sum(weekend) as weekend,
        ROUND(sum(weekend)/8,2)*100 as weekend_pct,
        sum(weekdays) as weekday,
        ROUND(sum(weekdays)/20, 2)*100 as weekday_pct,
        sum(events) as events,
        sum(sessions) as sessions,
        sum(views) as views,
        sum(views_from_search) as views_from_search, 
        sum(comments) as comments,
        sum(shares) as shares,
        sum(favourite_adds) as favourite_adds,
        sum(favourite_removes) as favourite_removes,
        sum(video_starts) as video_starts,
        sum(video_completes) AS video_completes,
        sum(dwell_mins) AS dwell_mins,
        'Recent 4 week usage' as Product_usage_duration
        FROM `{project_name}.tnl_engagement_metrics.tnl_12weeks_dwell_master_IJ` 
        where roll_cont_weeks in(14,21,28,35)
        group by userid,period_current,Device,Device_Type,Product
        """.format(project_name=project)
}

#saved as`{project_name}:tnl_engagement_metrics.tnl_4week_agg`
    DIG_12WEEK_USAGE = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12week_agg',
    'sql_query': """
        SELECT userid,period_current,Device,Device_Type,Product,min(Recency) AS recency,
        sum(active_days) as active_days,
        sum(weekend) as weekend,
        ROUND(sum(weekend)/26,2)*100 as weekend_pct,
        sum(weekdays) as weekday,
        ROUND(sum(weekdays)/65, 2)*100 as weekday_pct,
        sum(events) as events,
        sum(sessions) as sessions,
        sum(views) as views,
        sum(views_from_search) as views_from_search, 
        sum(comments) as comments,
        sum(shares) as shares,
        sum(favourite_adds) as favourite_adds,
        sum(favourite_removes) as favourite_removes,
        sum(video_starts) as video_starts,
        sum(video_completes) AS video_completes,
        sum(dwell_mins) AS dwell_mins,
        'Recent 12 week usage' as Product_usage_duration
        FROM `{project_name}.tnl_engagement_metrics.tnl_12weeks_dwell_master_IJ` 
        where roll_cont_weeks in(14,21,28,35,42,49,56,63,70,77,84,91)
        group by userid,period_current,Device,Device_Type,Product
        """.format(project_name=project)
}
#saved as`{project_name}:tnl_engagement_metrics.tnl_12week_agg`
#join to create  long form master table
    DIG_METRICS = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_dig_metrics',
    'sql_query': """
        select * from
        (select * from `{project_name}.tnl_engagement_metrics.tnl_1week_agg`) UNION ALL
        (select * from `{project_name}.tnl_engagement_metrics.tnl_2week_agg`) UNION ALL
        (select * from `{project_name}.tnl_engagement_metrics.tnl_4week_agg`) UNION ALL
        (select * from `{project_name}.tnl_engagement_metrics.tnl_12week_agg`)
        """.format(project_name=project)
}
#save as `{project_name}:tnl_engagement_metrics.tnl_dig_metrics`
#join to create combined long view with low/med/high ranking
#--------------------------------------------------------------------------------------
#PRODUCT USAGE - METRIC LONG
#--------------------------------------------------------------------------------------
    DIG_METRICS_CAT = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_dig_metrics_cat',
    'sql_query': """   
        SELECT *,
            CASE
              WHEN metric_value >= metric_value_high_pct THEN '3 - High'
              WHEN metric_value > metric_value_low_pct THEN '2 - Medium'
              ELSE '1 - Low' END AS metric_value_bin
            FROM

            (SELECT *,
            PERCENTILE_CONT(metric_value,0.33) OVER (PARTITION BY period_current,Device,Device_Type,Product_usage_duration,Product, metric_name ) AS metric_value_low_pct,
            PERCENTILE_CONT(metric_value,0.66) OVER (PARTITION BY period_current,Device,Device_Type,Product_usage_duration,Product, metric_name ) AS metric_value_high_pct

            FROM
            (SELECT * FROM

            (SELECT userid, period_current,Device,Device_Type,Product_usage_duration,Product,recency, 'Active Days' AS metric_name, active_days AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current,Device,Device_Type,Product_usage_duration,Product,recency, 'Weekend' AS metric_name, weekend AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current,Device,Device_Type,Product_usage_duration,Product,recency, 'Weekend_pct' AS metric_name, weekend_pct  AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL

            (SELECT userid, period_current,Device,Device_Type,Product_usage_duration,Product,recency, 'Weekday' AS metric_name, weekday AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current,Device,Device_Type,Product_usage_duration,Product,recency, 'Weekday_pct' AS metric_name, weekday_pct  AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Views' AS metric_name, views AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Events' AS metric_name, events AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Search Views' AS metric_name, views_from_search AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Comments' AS metric_name, comments AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Shares' AS metric_name, shares AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Favourite Adds' AS metric_name, favourite_adds AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Favourite Removes' AS metric_name, favourite_removes AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Video Starts' AS metric_name, video_starts AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Video Completes' AS metric_name, video_completes AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)
            UNION ALL
            (SELECT userid, period_current, Device,Device_Type,Product_usage_duration,Product,recency, 'Dwell_time' AS metric_name, dwell_mins  AS metric_value
            FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics`)

            )
            WHERE metric_value >0)
        """.format(project_name=project)
}

    query_list1 ['DIG_USAGE_OVERALL'] = DIG_USAGE_OVERALL
    query_list1 ['DIG_DWELL_OVERALL'] = DIG_DWELL_OVERALL
    query_list1 ['DIG_USAGE_MASTER'] = DIG_USAGE_MASTER
    query_list1 ['DIG_1WEEK_USAGE'] = DIG_1WEEK_USAGE
    query_list1 ['DIG_2WEEK_USAGE'] = DIG_2WEEK_USAGE
    query_list1 ['DIG_4WEEK_USAGE'] = DIG_4WEEK_USAGE
    query_list1 ['DIG_12WEEK_USAGE'] = DIG_12WEEK_USAGE
    query_list1 ['DIG_METRICS'] = DIG_METRICS
    query_list1 ['DIG_METRICS_CAT'] = DIG_METRICS_CAT
    return query_list1
    
#SAVE AS `{project_name}:tnl_engagement_metrics.tnl_dig_metrics_cat`

#churn outcome

#1week usage and the week after

DIG_1WEEK_OUTCOME = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_1week_outcome',
    'sql_query': """  
        SELECT *,
            CASE 
            WHEN active_current = 1 AND active_next = 1 THEN 0
            WHEN active_current = 1 AND active_next = 0 THEN 1
            END AS outcome_churn
            FROM
            (SELECT 
            cpn AS userid,
            cast(CURRENT_DATE() as date) AS period_current,
            'Recent 1 week usage' AS Product_usage_duration,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN cstart END) AS contract_start_date,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN (DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY)) END) AS Tenure_days,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/30.5),0) END) AS Tenure_months,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/365.25),1) END) AS Tenure_years,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN mpc END) AS mpc_current,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN printOrDigiFlag END) AS mpc_type_current,	
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_current,
            MAX(CASE WHEN eval_period = 'Next' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_next
            FROM
            (SELECT * FROM
            (SELECT 'Current' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -14 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -8 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Next' As eval_period, cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Temp' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' )) 
            GROUP BY userid, period_current)
            WHERE active_current = 1
        """
}
#save as `{project_name}:tnl_engagement_metrics.tnl_1week_outcome`


#2week usage and the week after
DIG_2WEEK_OUTCOME = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_2week_outcome',
    'sql_query': """  
        SELECT *,
            CASE 
            WHEN active_current = 1 AND active_next = 1 THEN 0
            WHEN active_current = 1 AND active_next = 0 THEN 1
            END AS outcome_churn
            FROM
            (SELECT 
            cpn AS userid,
            cast(CURRENT_DATE() as date) AS period_current,
            'Recent 2 week usage' AS Product_usage_duration,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN cstart END) AS contract_start_date,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN (DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY)) END) AS Tenure_days,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/30.5),0) END) AS Tenure_months,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/365.25),1) END) AS Tenure_years,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN mpc END) AS mpc_current,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN printOrDigiFlag END) AS mpc_type_current,	
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_current,
            MAX(CASE WHEN eval_period = 'Next' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_next
            FROM
            (SELECT * FROM
            (SELECT 'Current' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -21 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -8 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Next' As eval_period, cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Temp' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' )) 
            GROUP BY userid, period_current)
            WHERE active_current = 1
        """
}
#save as `{project_name}:tnl_engagement_metrics.tnl_2week_outcome`

#4week usage and the week after
DIG_4WEEK_OUTCOME = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_4week_outcome',
    'sql_query': """ 
        SELECT *,
            CASE 
            WHEN active_current = 1 AND active_next = 1 THEN 0
            WHEN active_current = 1 AND active_next = 0 THEN 1
            END AS outcome_churn
            FROM
            (SELECT 
            cpn AS userid,
            cast(CURRENT_DATE() as date) AS period_current,
            'Recent 4 week usage' AS Product_usage_duration,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN cstart END) AS contract_start_date,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN (DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY)) END) AS Tenure_days,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/30.5),0) END) AS Tenure_months,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/365.25),1) END) AS Tenure_years,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN mpc END) AS mpc_current,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN printOrDigiFlag END) AS mpc_type_current,	
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_current,
            MAX(CASE WHEN eval_period = 'Next' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_next
            FROM
            (SELECT * FROM
            (SELECT 'Current' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -8 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Next' As eval_period, cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Temp' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' )) 
            GROUP BY userid, period_current)
            WHERE active_current = 1
        """
}
#save as `{project_name}:tnl_engagement_metrics.tnl_4week_outcome`

#12week usage and the week after
DIG_12WEEK_OUTCOME = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12week_outcome',
    'sql_query': """ 
        SELECT *,
            CASE 
            WHEN active_current = 1 AND active_next = 1 THEN 0
            WHEN active_current = 1 AND active_next = 0 THEN 1
            END AS outcome_churn
            FROM
            (SELECT 
            cpn AS userid,
            cast(CURRENT_DATE() as date) AS period_current,
            'Recent 12 week usage' AS Product_usage_duration,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN cstart END) AS contract_start_date,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN (DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY)) END) AS Tenure_days,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/30.5),0) END) AS Tenure_months,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/365.25),1) END) AS Tenure_years,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN mpc END) AS mpc_current,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN printOrDigiFlag END) AS mpc_type_current,	
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_current,
            MAX(CASE WHEN eval_period = 'Next' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_next
            FROM
            (SELECT * FROM
            (SELECT 'Current' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -91 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -8 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Next' As eval_period, cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Temp' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' )) 
            GROUP BY userid, period_current)
            WHERE active_current = 1
        """
}

#12week usage and the week after
DIG_ALL_OUTCOME = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_all_outcome',
    'sql_query': """ 
        SELECT *,
            CASE 
            WHEN active_current = 1 AND active_next = 1 THEN 0
            WHEN active_current = 1 AND active_next = 0 THEN 1
            END AS outcome_churn
            FROM
            (SELECT 
            cpn AS userid,
            cast(CURRENT_DATE() as date) AS period_current,
            'Recent 1 week usage' AS Product_usage_duration,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN cstart END) AS contract_start_date,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN (DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY)) END) AS Tenure_days,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/30.5),0) END) AS Tenure_months,
            MAX(CASE WHEN eval_period = 'Temp' AND subscriptionStatusCode = 'Active' THEN round(((DATE_DIFF(cast(CURRENT_DATE() as date),cstart, DAY))/365.25),1) END) AS Tenure_years,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN mpc END) AS mpc_current,
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN printOrDigiFlag END) AS mpc_type_current,	
            MAX(CASE WHEN eval_period = 'Current' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_current,
            MAX(CASE WHEN eval_period = 'Next' AND subscriptionStatusCode = 'Active' THEN 1 ELSE 0 END) AS active_next
            FROM
            (SELECT * FROM
            (SELECT 'Current' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -91 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -8 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Next' As eval_period, cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' ) 
            union ALL
            (SELECT 'Temp' AS eval_period,cpn,subscriptionStatusCode,mpc,printOrDigiFlag,cast(substr(contractStartDate,1,10) as date) as cstart
            FROM `newsuk-datatech-prod.athena.accounts_csv_*`
            where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY))
            and substr(mpc,1,2)	in ('MP','TI') AND printOrDigiFlag IN ('Digital','Digi-Print','Print') and contractStartDate != '' )) 
            GROUP BY userid, period_current)
            WHERE active_current = 1
        """
}
#save as `{project_name}:tnl_engagement_metrics.tnl_12week_outcome`

#append all outcome status tables
def save_query2(project):
    query_list2 = {}
    DIG_OUTCOME_STATUS = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_outcome_status',
    'sql_query': """ 
        select * from 
        (select * FROM `{project_name}.tnl_engagement_metrics.tnl_1week_outcome`) UNION ALL
        (select * FROM `{project_name}.tnl_engagement_metrics.tnl_2week_outcome`) UNION ALL
        (select * FROM `{project_name}.tnl_engagement_metrics.tnl_4week_outcome`) UNION ALL
        (select * FROM `{project_name}.tnl_engagement_metrics.tnl_12week_outcome`) UNION ALL
        (select * FROM `{project_name}.tnl_engagement_metrics.tnl_all_outcome`)
        """.format(project_name=project)
}
#save as `{project}:tnl_engagement_metrics.tnl_outcome_status`
#Need to transpose data to get one line per user for usage period against metrics and their metric bin
    DIG_TRANSPOSE = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_transpose',
    'sql_query': """
        Select A.userid userid, A.period_current period_current,A.Product_usage_duration Product_usage_duration,A.Device Device,A.Device_type Device_type,A.Product Product,A.recency recency,
        A.act_days_value act_days_value,A.act_days_cat act_days_cat,ifnull(AA.weekend_value,0) weekend_value,(case when AA.weekend_value is null then '1 - Low' else AA.weekend_cat END) weekend_cat,
        ifnull(AB.weekday_value,0) weekday_value,(case when AB.weekday_value is null then '1 - Low' else AB.weekday_cat END) weekday_cat,ifnull(AC.weekend_pct_value,0) weekend_pct_value,(case when AC.weekend_pct_cat is null then '1 - Low' else AC.weekend_pct_cat END) weekend_pct_cat,
        ifnull(AD.weekday_pct_value,0) weekday_pct_value,(case when AD.weekday_pct_cat is null then '1 - Low' else AD.weekday_pct_cat END) weekday_pct_cat,ifnull(C.Events_value,0) Events_value,(case when C.Events_cat is null then '1 - Low' else C.Events_cat END) Events_cat ,
        ifnull(D.Views_value,0) Views_value,(case when D.Views_cat is null then '1 - Low' else D.Views_cat END) Views_cat,ifnull(E.Dwell_time_value,0) Dwell_time_value,(case when E.Dwell_time_cat is null then '1 - Low' else E.Dwell_time_cat END) Dwell_time_cat,
        ifnull(F.Search_Views_value,0) Search_Views_value,(case when F.Search_Views_cat is null then '1 - Low' else F.Search_Views_cat END) Search_Views_cat,ifnull(G.Comments_value,0) Comments_value,(case when G.Comments_cat is null then '1 - Low' else G.Comments_cat END) Comments_cat,
        ifnull(H.Shares_value,0) Shares_value,(case when H.Shares_cat is null then '1 - Low' else H.Shares_cat END) Shares_cat,ifnull(I.Video_Starts_value,0) Video_Starts_value,(case when I.Video_Starts_cat is null then '1 - Low' else I.Video_Starts_cat END) Video_Starts_cat,
        ifnull(J.Video_Completes_value,0) Video_Completes_value,(case when J.Video_Completes_cat is null then '1 - Low' else J.Video_Completes_cat END) Video_Completes_cat,ifnull(K.Favourite_Adds_value,0) Favourite_Adds_value,(case when K.Favourite_Adds_cat is null then '1 - Low' else K.Favourite_Adds_cat END) Favourite_Adds_cat,
        ifnull(L.Favourite_Removes_value,0) Favourite_Removes_value,(case when L.Favourite_Removes_cat is null then '1 - Low' else L.Favourite_Removes_cat END) Favourite_Removes_cat
        from
        (SELECT userid, period_current,Product_usage_duration,Device,Device_type,Product,recency, metric_value as act_days_value ,metric_value_bin as act_days_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Active Days') A
        left outer join 
        (SELECT userid, period_current,Product_usage_duration,Device,Device_type,Product,recency,metric_value as weekend_value,metric_value_bin as weekend_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Weekend') AA
        on A.userid=AA.userid and A.period_current=AA.period_current and A.Product_usage_duration=AA.Product_usage_duration and A.Device=AA.Device and A.Device_type=AA.Device_type and A.Product=AA.Product
        left outer join 
        (SELECT userid, period_current,Product_usage_duration,Device,Device_type,Product,recency,metric_value as weekday_value,metric_value_bin as weekday_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Weekday') AB
        on A.userid=AB.userid and A.period_current=AB.period_current and A.Product_usage_duration=AB.Product_usage_duration and A.Device=AB.Device and A.Device_type=AB.Device_type and A.Product=AB.Product
        left outer join
        (SELECT userid, period_current,Product_usage_duration,Device,Device_type,Product,recency,metric_value as weekend_pct_value,metric_value_bin as weekend_pct_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Weekend_pct') AC
        on A.userid=AC.userid and A.period_current=AC.period_current and A.Product_usage_duration=AC.Product_usage_duration and A.Device=AC.Device and A.Device_type=AC.Device_type and A.Product=AC.Product
        left outer join 
        (SELECT userid, period_current,Product_usage_duration,Device,Device_type,Product,recency,metric_value as weekday_pct_value,metric_value_bin as weekday_pct_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Weekday_pct') AD
        on A.userid=AD.userid and A.period_current=AD.period_current and A.Product_usage_duration=AD.Product_usage_duration and A.Device=AD.Device and A.Device_type=AD.Device_type and A.Product=AD.Product
        left outer join
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Events_value,metric_value_bin as Events_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Events') C
        on A.userid=C.userid and A.period_current=C.period_current and A.Product_usage_duration=C.Product_usage_duration and A.Device=C.Device and A.Device_type=C.Device_type and A.Product=C.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Views_value,metric_value_bin as Views_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Views') D
        on A.userid=D.userid and A.period_current=D.period_current and A.Product_usage_duration=D.Product_usage_duration and A.Device=D.Device and A.Device_type=D.Device_type and A.Product=D.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Dwell_time_value,metric_value_bin as Dwell_time_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Dwell_time') E
        on A.userid=E.userid and A.period_current=E.period_current and A.Product_usage_duration=E.Product_usage_duration and A.Device=E.Device and A.Device_type=E.Device_type and A.Product=E.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Search_Views_value,metric_value_bin as Search_Views_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Search Views') F
        on A.userid=F.userid and A.period_current=F.period_current and A.Product_usage_duration=F.Product_usage_duration and A.Device=F.Device and A.Device_type=F.Device_type and A.Product=F.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Comments_value,metric_value_bin as Comments_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Comments') G
        on A.userid=G.userid and A.period_current=G.period_current and A.Product_usage_duration=G.Product_usage_duration and A.Device=G.Device and A.Device_type=G.Device_type and A.Product=G.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Shares_value,metric_value_bin as Shares_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Shares') H
        on A.userid=H.userid and A.period_current=H.period_current and A.Product_usage_duration=H.Product_usage_duration and A.Device=H.Device and A.Device_type=H.Device_type and A.Product=H.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Video_Starts_value,metric_value_bin as Video_Starts_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Video Starts') I
        on A.userid=I.userid and A.period_current=I.period_current and A.Product_usage_duration=I.Product_usage_duration and A.Device=I.Device and A.Device_type=I.Device_type and A.Product=I.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Video_Completes_value,metric_value_bin as Video_Completes_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Video Completes') J
        on A.userid=J.userid and A.period_current=J.period_current and A.Product_usage_duration=J.Product_usage_duration and A.Device=J.Device and A.Device_type=J.Device_type and A.Product=J.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Favourite_Adds_value,metric_value_bin as Favourite_Adds_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Favourite Adds') K
        on A.userid=K.userid and A.period_current=K.period_current and A.Product_usage_duration=K.Product_usage_duration and A.Device=K.Device and A.Device_type=K.Device_type and A.Product=K.Product
        left outer join 
        (SELECT userid,period_current,Product_usage_duration,Device,Device_type,Product,metric_value as Favourite_Removes_value,metric_value_bin as Favourite_Removes_cat
        FROM `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat` 
        where metric_name='Favourite Removes') L
        on A.userid=L.userid and A.period_current=L.period_current and A.Product_usage_duration=L.Product_usage_duration and A.Device=L.Device and A.Device_type=L.Device_type and A.Product=L.Product
        #where A.userid in ('AAAA003110137') 
         """.format(project_name=project)
}
#save as `{project_name}:tnl_engagement_metrics.tnl_transpose`
#joining metrics and link to outcome:Brings back only all with  digital usage
    DIG_OUTCOME_LINKED = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_engagement_rollup',
    'sql_query': """ 
        SELECT * FROM
        (SELECT
        A.userid  AS  userid,
        cast(A.period_current as string)  AS  period_current,
        A.Product_usage_duration  AS  Product_usage_duration,
        A.Device  AS  Device,
        A.Device_type  AS  Device_type,
        A.Product  AS  Product,
        cast(A.recency as INT64) AS  recency ,
        cast(A.act_days_value as INT64)  AS  act_days_value,
        A.act_days_cat  AS  act_days_cat,
        cast(A.weekend_value as INT64) AS  weekend_value,
        A.weekend_cat  AS  weekend_cat,
        cast(A.weekend_pct_value as INT64) AS  weekend_pct_value,
        A.weekend_pct_cat  AS  weekend_pct_cat,
        cast(A.weekday_value as INT64) AS  weekday_value,
        A.weekday_cat  AS  weekday_cat,
        cast(A.weekday_pct_value as INT64) AS  weekday_pct_value,
        A.weekday_pct_cat  AS  weekday_pct_cat,
        cast(A.Events_value  AS  INT64) as Events_value,
        A.Events_cat  AS  Events_cat,
        cast(A.Views_value as INT64) AS  Views_value,
        A.Views_cat  AS  Views_cat,
        cast(A.Dwell_time_value as INT64) AS  Dwell_time_value,
        A.Dwell_time_cat  AS  Dwell_time_cat,
        cast(A.Search_Views_value as INT64) AS  Search_Views_value,
        A.Search_Views_cat  AS  Search_Views_cat,
        cast(A.Comments_value as INT64) AS  Comments_value,
        A.Comments_cat  AS  Comments_cat,
        cast(A.Shares_value as INT64) AS  Shares_value,
        A.Shares_cat  AS  Shares_cat,
        cast(A.Video_Starts_value as INT64) AS  Video_Starts_value,
        A.Video_Starts_cat  AS  Video_Starts_cat,
        cast(A.Video_Completes_value as INT64) AS  Video_Completes_value,
        A.Video_Completes_cat  AS  Video_Completes_cat,
        cast(A.Favourite_Adds_value as INT64) AS  Favourite_Adds_value,
        A.Favourite_Adds_cat  AS  Favourite_Adds_cat,
        cast(A.Favourite_Removes_value as INT64) AS  Favourite_Removes_value,
        A.Favourite_Removes_cat  AS  Favourite_Removes_cat,
        cast(ifnull(C.contract_start_date,'9999-12-31') as string) as C_contract_start_date,
        ifnull(C.Tenure_days,0) as C_Tenure_days,
        ifnull(C.Tenure_months,0) as C_Tenure_months,
        ifnull(C.Tenure_years,0) as C_Tenure_years,
        ifnull(C.mpc_current,'Unknown') mpc_current,
        ifnull(C.mpc_type_current,'Unknown') mpc_type_current,
        ifnull(C.outcome_churn,99) outcome_churn
        FROM
        `{project_name}.tnl_engagement_metrics.tnl_transpose` A
        LEFT OUTER JOIN 
        `{project_name}.tnl_engagement_metrics.tnl_outcome_status` C
        ON A.userid = C.userid AND A.period_current = C.period_current AND A.Product_usage_duration=C.Product_usage_duration
        where outcome_churn IS NOT NULL and A.period_current= cast(CURRENT_DATE() as date))
        #WHERE outcome_churn IS NOT NULL
        """.format(project_name=project)
}
#save as `{project_name}:tnl_engagement_metrics.tnl_engagement_rollup`
#create output tables just for current refresh that needs to be moved into Athena
    DIG_ENG_OUTPUT = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_engagement_current',
    'sql_query': """
        select * from `{project_name}.tnl_engagement_metrics.tnl_engagement_rollup` 
        where cast(period_current as date) = cast(CURRENT_DATE() as date)
        """.format(project_name=project)
}
    query_list2 ['DIG_OUTCOME_STATUS'] = DIG_OUTCOME_STATUS
    query_list2 ['DIG_TRANSPOSE'] = DIG_TRANSPOSE
    query_list2 ['DIG_OUTCOME_LINKED'] = DIG_OUTCOME_LINKED
    query_list2 ['DIG_ENG_OUTPUT'] = DIG_ENG_OUTPUT
    return query_list2


#Add section

DIG_SECTION_MASTER = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_section_tab_ph_desk',
    'sql_query': """ 
        #need to create master dataset of cms tables and NLA table to get all article sections
        #phone/desktop related article sections
        select Key,Section from 
        ((Select Key,case when lower(SectionLevel2) LIKE ('%news%') then 'News'  
        when  lower(SectionLevel2) LIKE ('%front%') THEN 'News'
        when lower(SectionLevel2) LIKE ('%business%') then 'Business'  
        when lower(SectionLevel2) LIKE  ('%life%') then 'Life'  
        when lower(SectionLevel2) LIKE  ('%mindgames%') then 'Mindgames' 
        when lower(SectionLevel2) LIKE  ('%opinion%') then 'Opinion' 
        when lower(SectionLevel2) LIKE  ('%sport%') then 'Sport' 
        when lower(SectionLevel2) LIKE  ('%comment%') then 'Comment' 
        when lower(SectionLevel2) LIKE  ('%arts%') then 'Arts' 
        when lower(SectionLevel2) LIKE  ('%culture%') then 'Culture' 
        when lower(SectionLevel2) LIKE  ('%driving%') then 'Driving' 
        when lower(SectionLevel2) LIKE  ('%home%') then 'Home' 
        when lower(SectionLevel2) LIKE  ('%magazine%') then 'Magazine' 
        when lower(SectionLevel2) LIKE  ('%money%') then 'Money' 
        when lower(SectionLevel2) LIKE  ('%news review%') then 'News_review' 
        when lower(SectionLevel2) LIKE  ('%puzzles%') then 'Puzzles' 
        when lower(SectionLevel2) LIKE  ('%register%') then 'Register' 
        when lower(SectionLevel2) LIKE  ('%saturday review%') then 'Saturday_review' 
        when lower(SectionLevel2) LIKE  ('%style%') then 'Style' 
        when lower(SectionLevel2) LIKE  ('%the game%') then 'The_game' 
        when lower(SectionLevel2) LIKE  ('%times+%') then 'Times+' 
        when lower(SectionLevel2) LIKE  ('%times2%') then 'Times2' 
        when lower(SectionLevel2) LIKE  ('%weekend%') then 'Weekend' 
        when lower(SectionLevel2) LIKE  ('%world%') then 'World' 
        when lower(SectionLevel2) LIKE  ('%travel%') then 'Travel' 
        when lower(SectionLevel2) LIKE  ('%scotland%') then 'Scotland' 
        when lower(SectionLevel2) LIKE  ('%ireland%') then 'Ireland' 
        when lower(SectionLevel2) LIKE  ('%law%') then 'Law'
        ELSE 'Other' END as Section from `newsuk-datatech-prod.cms.times_*`
        where _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -150 DAY)) AND FORMAT_DATE("%Y%m%d", DATE_ADD(CURRENT_DATE(), INTERVAL -8 DAY))
        ) 
        UNION ALL
        #tablet related article sections
        (Select articleId as Key, case when lower(sectionName) LIKE ('%news%') then 'News'  
        when lower(sectionName) LIKE ('%business%') then 'Business'  
        when lower(sectionName) LIKE  ('%life%') then 'Life'  
        when lower(sectionName) LIKE  ('%mindgames%') then 'Mindgames' 
        when lower(sectionName) LIKE  ('%opinion%') then 'Opinion' 
        when lower(sectionName) LIKE  ('%sport%') then 'Sport' 
        when lower(sectionName) LIKE  ('%comment%') then 'Comment' 
        when lower(sectionName) LIKE  ('%arts%') then 'Arts' 
        when lower(sectionName) LIKE  ('%culture%') then 'Culture' 
        when lower(sectionName) LIKE  ('%driving%') then 'Driving' 
        when lower(sectionName) LIKE  ('%home%') then 'Home' 
        when lower(sectionName) LIKE  ('%magazine%') then 'Magazine' 
        when lower(sectionName) LIKE  ('%money%') then 'Money' 
        when lower(sectionName) LIKE  ('%news review%') then 'News_review' 
        when lower(sectionName) LIKE  ('%puzzles%') then 'Puzzles' 
        when lower(sectionName) LIKE  ('%register%') then 'Register' 
        when lower(sectionName) LIKE  ('%saturday review%') then 'Saturday_review' 
        when lower(sectionName) LIKE  ('%style%') then 'Style' 
        when lower(sectionName) LIKE  ('%the game%') then 'The_game' 
        when lower(sectionName) LIKE  ('%times+%') then 'Times+' 
        when lower(sectionName) LIKE  ('%times2%') then 'Times2' 
        when lower(sectionName) LIKE  ('%weekend%') then 'Weekend' 
        when lower(sectionName) LIKE  ('%world%') then 'World' 
        when lower(sectionName) LIKE  ('%travel%') then 'Travel' 
        when lower(sectionName) LIKE  ('%scotland%') then 'Scotland' 
        when lower(sectionName) LIKE  ('%ireland%') then 'Ireland'
        when lower(sectionName) LIKE  ('%law%') then 'Law'
        ELSE 'Other' END as Section 
        from `newsuk-datatech-prod.NLAConverter.Article`)) 
        group by Key,Section
        
        """
}

def save_query3(project):
    query_list3 = {}
    DIG_SECTION_USER = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_section_user',
    'sql_query': """ 
         Select userid,period_current,period_active,roll_cont_weeks,final_Section,count(distinct Key) as art_cnt from
        (SELECT met.userid,met.period_current,met.period_active,met.roll_cont_weeks,met.Key,case when lower(product) in ('times puzzles','the sunday times puzzles') then 'Puzzles'
        when lower(product) in ('times+') then 'Times+'
        WHEN Section is null then 'Other' else Section end As final_Section
        FROM (select customer_id as userid,
        cast(CURRENT_DATE() as date)  as period_current,
        date_add(cast(date_TRUNC(cast(activity_date as DATE), WEEK(MONDAY)) as date), INTERVAL 6 DAY) AS period_active,
        DATE_DIFF(CURRENT_DATE(), cast(activity_date as date), DAY) as roll_cont_weeks,
        content_id as Key,
        product
        FROM `newsuk-datatech-prod.inca_clickstream_tables.times_clickstream_daily`
        WHERE timestamp(activity_date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as TIMESTAMP)  AND timestamp(activity_date)  <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as TIMESTAMP) and
        cast(activity_date as date) >= cast(DATE_add(current_date(), INTERVAL -91 DAY) as date) AND cast(activity_date as date) <= cast(DATE_add(current_date(), INTERVAL -8 DAY) as date)
        and customer_id is not null AND product IS NOT NULL and
        lower(product) in ("t&st android app","t&st android app - irish app","t&st android app beta","t&st iphone app","t&st iphone app - irish app","t&st iphone app - times of london app",
        "the sunday times","the sunday times puzzles","the times","the times - ireland","the times acquisition store","the times acquisition store - ireland","the times acquisition store - uk",
        "the times and sunday times","the times and sunday times android app","the times and sunday times android app - irish app","the times and sunday times android tablet app",
        "the times and sunday times android tablet app - irish app","the times and sunday times ipad app","the times and sunday times ipad app - irish app","the times and sunday times iphone app",
        "the times and sunday times kindle app","the times city guides site","the times help hub site","the times ireland membership site","the times politics site","the times sport android app",
        "the times sport ios app","times puzzles","times+","the times and sunday times iphone app - irish app")
        group by userid,period_current,period_active,roll_cont_weeks,Key,product) as met
        LEFT OUTER join
        (Select * from `{project_name}.tnl_engagement_metrics.tnl_section_tab_ph_desk`) as sec
        on met.Key=sec.Key)
        group by userid,period_current,period_active,roll_cont_weeks,final_Section
        """.format(project_name=project)
}
#save as `tnl_section_test`
    DIG_SECTION_FINAL = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_section_final',
    'sql_query': """ 
        Select * from
        (Select coalesce(A.userid,B.userid,C.userid,D.userid,E.userid,F.userid,G.userid,H.userid,I.userid,J.userid,K.userid,L.userid,M.userid,N.userid,O.userid
        ,P.userid,Q.userid,R.userid,S.userid,T.userid,U.userid,X.userid,Y.userid,Z.userid,AA.userid,AB.userid) userid,
        coalesce(A.period_current,B.period_current,C.period_current,D.period_current,E.period_current,F.period_current,G.period_current,H.period_current,I.period_current,J.period_current,K.period_current,L.period_current,M.period_current,N.period_current,O.period_current
        ,P.period_current,Q.period_current,R.period_current,S.period_current,T.period_current,U.period_current,X.period_current,Y.period_current,Z.period_current,AA.period_current,AB.period_current) period_current,
        coalesce(A.period_active,B.period_active,C.period_active,D.period_active,E.period_active,F.period_active,G.period_active,H.period_active,I.period_active,J.period_active,K.period_active,L.period_active,M.period_active,N.period_active,O.period_active
        ,P.period_active,Q.period_active,R.period_active,S.period_active,T.period_active,U.period_active,X.period_active,Y.period_active,Z.period_active,AA.period_active,AB.period_active) period_active,
        coalesce(A.roll_cont_weeks,B.roll_cont_weeks,C.roll_cont_weeks,D.roll_cont_weeks,E.roll_cont_weeks,F.roll_cont_weeks,G.roll_cont_weeks,H.roll_cont_weeks,I.roll_cont_weeks,J.roll_cont_weeks,K.roll_cont_weeks,L.roll_cont_weeks,M.roll_cont_weeks,N.roll_cont_weeks,O.roll_cont_weeks
        ,P.roll_cont_weeks,Q.roll_cont_weeks,R.roll_cont_weeks,S.roll_cont_weeks,T.roll_cont_weeks,U.roll_cont_weeks,X.roll_cont_weeks,Y.roll_cont_weeks,Z.roll_cont_weeks,AA.roll_cont_weeks,AB.roll_cont_weeks) roll_cont_weeks,
        ifnull(A.News_articles,0) News_articles,ifnull(B.Biz_articles,0) Biz_articles,ifnull(C.Life_articles,0) Life_articles,ifnull(D.Mindgames_articles,0) MG_articles,
        ifnull(E.Opinion_articles,0) Opinion_articles,ifnull(F.Sport_articles,0) Sport_articles,ifnull(G.Comment_articles,0) Comment_articles,ifnull(H.Arts_articles,0) Arts_articles,
        ifnull(I.Culture_articles,0) Culture_articles,ifnull(J.Driving_articles,0) Driving_articles,ifnull(K.Home_articles,0) Home_articles,ifnull(L.Magazine_articles,0) Magazine_articles,ifnull(M.Money_articles,0) Money_articles,
        ifnull(N.News_review_articles,0) News_review_articles,ifnull(O.Puzzles_articles,0) Puzzles_articles,ifnull(P.Register_articles,0) Register_articles,ifnull(Q.Saturday_review_articles,0) Sat_review_articles,ifnull(R.Style_articles,0) Style_articles,ifnull(S.The_game_articles,0) The_game_articles,
        ifnull(T.Timesplus_articles,0) Timesplus_articles,ifnull(U.Times2_articles,0) Times2_articles,ifnull(V.Weekend_articles,0) Weekend_articles,ifnull(W.World_articles,0) World_articles,ifnull(X.Travel_articles,0) Travel_articles,ifnull(Y.Scotland_articles,0) Scotland_articles,ifnull(Z.Ireland_articles,0) Ireland_articles,
        ifnull(AB.Law_articles,0) Law_articles,ifnull(AA.Other_articles,0) Other_articles
        FROM (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS News_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('News')) as A
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Biz_articles 
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Business')) as B
        on A.userid=B.userid
        AND A.period_current=B.period_current
        AND A.period_active=B.period_active
        AND A.roll_cont_weeks=B.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Life_articles 
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Life')) as C
        on A.userid=C.userid
        AND A.period_current=C.period_current
        AND A.period_active=C.period_active
        AND A.roll_cont_weeks=C.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Mindgames_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Mindgames')) as D
        on A.userid=D.userid
        AND A.period_current=D.period_current
        AND A.period_active=D.period_active
        AND A.roll_cont_weeks=D.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Opinion_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Opinion')) as E
        on A.userid=E.userid
        AND A.period_current=E.period_current
        AND A.period_active=E.period_active
        AND A.roll_cont_weeks=E.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Sport_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Sport')) as F
        on A.userid=F.userid
        AND A.period_current=F.period_current
        AND A.period_active=F.period_active
        AND A.roll_cont_weeks=F.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Comment_articles 
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Comment')) as G
        on A.userid=G.userid
        AND A.period_current=G.period_current
        AND A.period_active=G.period_active
        AND A.roll_cont_weeks=G.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Arts_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Arts')) as H
        on A.userid=H.userid
        AND A.period_current=H.period_current
        AND A.period_active=H.period_active
        AND A.roll_cont_weeks=H.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Culture_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Culture')) as I
        on A.userid=I.userid
        AND A.period_current=I.period_current
        AND A.period_active=I.period_active
        AND A.roll_cont_weeks=I.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Driving_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Driving')) as J
        on A.userid=J.userid
        AND A.period_current=J.period_current
        AND A.period_active=J.period_active
        AND A.roll_cont_weeks=J.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Home_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Home')) as K
        on A.userid=K.userid
        AND A.period_current=K.period_current
        AND A.period_active=K.period_active
        AND A.roll_cont_weeks=K.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Magazine_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Magazine')) as L
        on A.userid=L.userid
        AND A.period_current=L.period_current
        AND A.period_active=L.period_active
        AND A.roll_cont_weeks=L.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Money_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Money')) as M
        on A.userid=M.userid
        AND A.period_current=M.period_current
        AND A.period_active=M.period_active
        AND A.roll_cont_weeks=M.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS News_review_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('News_review')) as N
        on A.userid=N.userid
        AND A.period_current=N.period_current
        AND A.period_active=N.period_active
        AND A.roll_cont_weeks=N.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Puzzles_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Puzzles')) as O
        on A.userid=O.userid
        AND A.period_current=O.period_current
        AND A.period_active=O.period_active
        AND A.roll_cont_weeks=O.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Register_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Register')) as P
        on A.userid=P.userid
        AND A.period_current=P.period_current
        AND A.period_active=P.period_active
        AND A.roll_cont_weeks=P.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Saturday_review_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Saturday_review')) as Q
        on A.userid=Q.userid
        AND A.period_current=Q.period_current
        AND A.period_active=Q.period_active
        AND A.roll_cont_weeks=Q.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Style_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Style')) as R
        on A.userid=R.userid
        AND A.period_current=R.period_current
        AND A.period_active=R.period_active
        AND A.roll_cont_weeks=R.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS The_game_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('The_game')) as S
        on A.userid=S.userid
        AND A.period_current=S.period_current
        AND A.period_active=S.period_active
        AND A.roll_cont_weeks=S.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Timesplus_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Times+')) as T
        on A.userid=T.userid
        AND A.period_current=T.period_current
        AND A.period_active=T.period_active
        AND A.roll_cont_weeks=T.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Times2_articles  
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Times2')) as U
        on A.userid=U.userid
        AND A.period_current=U.period_current
        AND A.period_active=U.period_active
        AND A.roll_cont_weeks=U.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Weekend_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Weekend')) as V
        on A.userid=V.userid
        AND A.period_current=V.period_current
        AND A.period_active=V.period_active
        AND A.roll_cont_weeks=V.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS World_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('World')) as W
        on A.userid=W.userid
        AND A.period_current=W.period_current
        AND A.period_active=W.period_active
        AND A.roll_cont_weeks=W.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Travel_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Travel')) as X
        on A.userid=X.userid
        AND A.period_current=X.period_current
        AND A.period_active=X.period_active
        AND A.roll_cont_weeks=X.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Scotland_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Scotland')) as Y
        on A.userid=Y.userid
        AND A.period_current=Y.period_current
        AND A.period_active=Y.period_active
        AND A.roll_cont_weeks=Y.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Ireland_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Ireland')) as Z
        on A.userid=Z.userid
        AND A.period_current=Z.period_current
        AND A.period_active=Z.period_active
        AND A.roll_cont_weeks=Z.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Law_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Law')) as AB
        on A.userid=AB.userid
        AND A.period_current=AB.period_current
        AND A.period_active=AB.period_active
        AND A.roll_cont_weeks=AB.roll_cont_weeks
        full outer join
        (SELECT userid, period_current, period_active,roll_cont_weeks,art_cnt AS Other_articles   
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_user`
        WHERE final_Section in ('Other')) as AA
        on A.userid=AA.userid
        AND A.period_current=AA.period_current
        AND A.period_active=AA.period_active
        AND A.roll_cont_weeks=AA.roll_cont_weeks)
        where userid is not null
        """.format(project_name=project)
}
#save as `tnl_section_final`
#create 1week/2weeks/4weeks
#1week usage
    DIG_1WEEK_SECTION = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_1week_section',
    'sql_query': """
        SELECT userid,period_current,'Recent 1 week usage' as Product_usage_duration,
        sum(News_articles) News_articles,sum(Biz_articles) Biz_articles,sum(Life_articles) Life_articles,sum(MG_articles) MG_articles,sum(Opinion_articles) Opinion_articles,sum(Sport_articles) Sport_articles,sum(Comment_articles) Comment_articles,
        sum(Arts_articles) Arts_articles,sum(Culture_articles) Culture_articles,sum(Driving_articles) Driving_articles,sum(Home_articles) Home_articles,sum(Magazine_articles) Magazine_articles,
        sum(Money_articles) Money_articles,sum(News_review_articles) News_review_articles,sum(Puzzles_articles) Puzzles_articles,sum(Register_articles) Register_articles,
        sum(Sat_review_articles) Sat_review_articles,sum(Style_articles) Style_articles,sum(The_game_articles) The_game_articles,sum(Timesplus_articles) Timesplus_articles,
        sum(Times2_articles) Times2_articles,sum(Weekend_articles) Weekend_articles,sum(World_articles) World_articles,sum(Travel_articles) Travel_articles,sum(Scotland_articles) Scotland_articles,
        sum(Ireland_articles) Ireland_articles,sum(Law_articles) Law_articles,sum(Other_articles) Other_articles
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_final` 
        where roll_cont_weeks in(14)
        group by userid,period_current
        """.format(project_name=project)
}
#save as `tnl_1week_Section`
#2 week usage
    DIG_2WEEK_SECTION = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_2week_section',
    'sql_query': """
        SELECT userid,period_current,'Recent 2 week usage' as Product_usage_duration,
        sum(News_articles) News_articles,sum(Biz_articles) Biz_articles,sum(Life_articles) Life_articles,sum(MG_articles) MG_articles,sum(Opinion_articles) Opinion_articles,sum(Sport_articles) Sport_articles,sum(Comment_articles) Comment_articles,
        sum(Arts_articles) Arts_articles,sum(Culture_articles) Culture_articles,sum(Driving_articles) Driving_articles,sum(Home_articles) Home_articles,sum(Magazine_articles) Magazine_articles,
        sum(Money_articles) Money_articles,sum(News_review_articles) News_review_articles,sum(Puzzles_articles) Puzzles_articles,sum(Register_articles) Register_articles,
        sum(Sat_review_articles) Sat_review_articles,sum(Style_articles) Style_articles,sum(The_game_articles) The_game_articles,sum(Timesplus_articles) Timesplus_articles,
        sum(Times2_articles) Times2_articles,sum(Weekend_articles) Weekend_articles,sum(World_articles) World_articles,sum(Travel_articles) Travel_articles,sum(Scotland_articles) Scotland_articles,
        sum(Ireland_articles) Ireland_articles,sum(Law_articles) Law_articles,sum(Other_articles) Other_articles
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_final` 
        where roll_cont_weeks in(14,21)
        group by userid,period_current
        """.format(project_name=project)
}
#save as `tnl_2week_Section`
#4 week usage
    DIG_4WEEK_SECTION = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_4week_section',
    'sql_query': """
        SELECT userid,period_current,'Recent 4 week usage' as Product_usage_duration,
        sum(News_articles) News_articles,sum(Biz_articles) Biz_articles,sum(Life_articles) Life_articles,sum(MG_articles) MG_articles,sum(Opinion_articles) Opinion_articles,sum(Sport_articles) Sport_articles,sum(Comment_articles) Comment_articles,
        sum(Arts_articles) Arts_articles,sum(Culture_articles) Culture_articles,sum(Driving_articles) Driving_articles,sum(Home_articles) Home_articles,sum(Magazine_articles) Magazine_articles,
        sum(Money_articles) Money_articles,sum(News_review_articles) News_review_articles,sum(Puzzles_articles) Puzzles_articles,sum(Register_articles) Register_articles,
        sum(Sat_review_articles) Sat_review_articles,sum(Style_articles) Style_articles,sum(The_game_articles) The_game_articles,sum(Timesplus_articles) Timesplus_articles,
        sum(Times2_articles) Times2_articles,sum(Weekend_articles) Weekend_articles,sum(World_articles) World_articles,sum(Travel_articles) Travel_articles,sum(Scotland_articles) Scotland_articles,
        sum(Ireland_articles) Ireland_articles,sum(Law_articles) Law_articles,sum(Other_articles) Other_articles
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_final` 
        where roll_cont_weeks in (14,21,28,35)
        group by userid,period_current
        """.format(project_name=project)
}
#save as `tnl_4week_Section`
#12 week usage
    DIG_12WEEK_SECTION = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_12week_section',
    'sql_query': """
        SELECT userid,period_current,'Recent 12 week usage' as Product_usage_duration,
        sum(News_articles) News_articles,sum(Biz_articles) Biz_articles,sum(Life_articles) Life_articles,sum(MG_articles) MG_articles,sum(Opinion_articles) Opinion_articles,sum(Sport_articles) Sport_articles,sum(Comment_articles) Comment_articles,
        sum(Arts_articles) Arts_articles,sum(Culture_articles) Culture_articles,sum(Driving_articles) Driving_articles,sum(Home_articles) Home_articles,sum(Magazine_articles) Magazine_articles,
        sum(Money_articles) Money_articles,sum(News_review_articles) News_review_articles,sum(Puzzles_articles) Puzzles_articles,sum(Register_articles) Register_articles,
        sum(Sat_review_articles) Sat_review_articles,sum(Style_articles) Style_articles,sum(The_game_articles) The_game_articles,sum(Timesplus_articles) Timesplus_articles,
        sum(Times2_articles) Times2_articles,sum(Weekend_articles) Weekend_articles,sum(World_articles) World_articles,sum(Travel_articles) Travel_articles,sum(Scotland_articles) Scotland_articles,
        sum(Ireland_articles) Ireland_articles,sum(Law_articles) Law_articles,sum(Other_articles) Other_articles
        FROM `{project_name}.tnl_engagement_metrics.tnl_section_final` 
        where roll_cont_weeks in (14,21,28,35,42,49,56,63,70,77,84,91)
        group by userid,period_current
        """.format(project_name=project)
}
#save as `tnl_12week_section`
#join to create  long form master table
    DIG_SECTION_AGG = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_section_agg',
    'sql_query': """
        select * from
        (select * from `{project_name}.tnl_engagement_metrics.tnl_1week_section`) UNION ALL
        (select * from `{project_name}.tnl_engagement_metrics.tnl_2week_section`) UNION ALL
        (select * from `{project_name}.tnl_engagement_metrics.tnl_4week_section`) UNION ALL
        (select * from `{project_name}.tnl_engagement_metrics.tnl_12week_section`) 
        """.format(project_name=project)
}
#save as `tnl_section_agg`
#merge with outcome_linked with section only where 'All'
    DIG_METRICS_SECTION = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_section_rollup',
    'sql_query': """
        SELECT userid ,period_current,Product_usage_duration,Device ,Device_type,Product,
            recency,act_days_value,act_days_cat,weekend_value, weekend_cat,weekend_pct_value,
             weekend_pct_cat,weekday_value,weekday_cat,weekday_pct_value,weekday_pct_cat,Events_value,
            Events_cat, Views_value,Views_cat,Dwell_time_value, Dwell_time_cat,Search_Views_value,
            Search_Views_cat,Comments_value,Comments_cat,Shares_value,Shares_cat, Video_Starts_value,
            Video_Starts_cat,Video_Completes_value,Video_Completes_cat,Favourite_Adds_value,Favourite_Adds_cat,
            Favourite_Removes_value,Favourite_Removes_cat,
            ifnull(C_contract_start_date,'9999-12-31')  AS  contract_start_date,
            ifnull(C_Tenure_days,0)  AS  Tenure_days,ifnull(C_Tenure_months,0)  AS  Tenure_months,
            ifnull(C_Tenure_years,0)  AS  Tenure_years,ifnull(mpc_current,'Unknown')  AS  mpc_current,ifnull(mpc_type_current,'Unknown')  AS  mpc_type_current,ifnull(outcome_churn,99)  AS  outcome_churn,
            ifnull(News_articles,0)  AS  News_articles,
            ifnull(Biz_articles,0)  AS  Biz_articles,ifnull(Life_articles,0)  AS  Life_articles,ifnull(MG_articles,0)  AS  MG_articles,ifnull(Opinion_articles,0)  AS  Opinion_articles,ifnull(Sport_articles,0)  AS  Sport_articles,ifnull(Comment_articles,0)  AS
            Comment_articles,
            ifnull(Arts_articles,0)  AS  Arts_articles,ifnull(Culture_articles,0)  AS  Culture_articles,ifnull(Driving_articles,0)  AS  Driving_articles,ifnull(Home_articles,0)  AS  Home_articles,ifnull(Magazine_articles,0)  AS  Magazine_articles,
            ifnull(Money_articles,0)  AS  Money_articles,
            ifnull(News_review_articles,0)  AS  News_review,ifnull(Puzzles_articles,0)  AS  Puzzles_articles,ifnull(Register_articles,0)  AS  Register_articles,
            ifnull(Sat_review_articles,0)  AS  Sat_review,ifnull(Style_articles,0)  AS  Style_articles,ifnull(The_game_articles,0)  AS  The_game,
            ifnull(Timesplus_articles,0)  AS  Timesplus_articles,ifnull(Times2_articles,0)  AS  Times2_articles,ifnull(Weekend_articles,0)  AS  Weekend_articles,ifnull(World_articles,0)  AS  World_articles,ifnull(Travel_articles,0)  AS  Travel_articles,ifnull(
            Scotland_articles,0)  AS  Scotland_articles,
            ifnull(Ireland_articles,0)  AS  Ireland_articles,ifnull(Law_articles,0)  AS  Law_articles,ifnull(Other_articles,0)  AS  Other_articles FROM
            (SELECT A.*,C.* except(userid, period_current, Product_usage_duration) FROM  `{project_name}.tnl_engagement_metrics.tnl_engagement_rollup`  A
            LEFT outer join
            `{project_name}.tnl_engagement_metrics.tnl_section_agg` C
            ON A.userid = C.userid AND cast(A.period_current as date) = C.period_current AND A.Product_usage_duration=C.Product_usage_duration
            WHERE A.Product in ('All') and cast(A.period_current as date)=cast(CURRENT_DATE() as date))
        #A.userid in ('AAAA003110137') and
        """.format(project_name=project)
}
    DIG_SEC_OUTPUT = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_section_current',
    'sql_query': """
        select * from `{project_name}.tnl_engagement_metrics.tnl_section_rollup` 
        where cast(period_current as date) = cast(CURRENT_DATE() as date)
        """.format(project_name=project)
}
#create data for dashboard
#restrict to just subscribers  this needs to append to existing dataset
    DIG_DASH_METRICS = {
    'dataset': 'tnl_engagement_metrics',
    'table_name': 'tnl_engdash_metrics',
    'sql_query': """
        select userid, period_current, Product_usage_duration, mpc_current, mpc_type_current,outcome_churn,
        (case when Device is null then 'No usage' ELSE  Device END) as Device,
        (case when Device_Type is null then 'No usage' ELSE  Device_Type END) as Device_Type,
        (case when Product is null then 'No usage' ELSE  Product END) as Product,
        (case when metric_name is null then 'No usage' ELSE  metric_name END) as metric_name,
        (case when metric_value is null then 0 ELSE  metric_value END) as metric_value,
        (case when metric_value_bin is null then 'No usage' ELSE  metric_value_bin END) as metric_value_bin
        from(
        SELECT A.userid, A.period_current, A.Product_usage_duration, A.mpc_current, A.mpc_type_current,A.outcome_churn,B.Device,
        B.Device_Type,B.Product, B.recency, B.metric_name, B.metric_value, B.metric_value_low_pct, B.metric_value_high_pct, B.metric_value_bin
        from
        (Select * FROM `{project_name}.tnl_engagement_metrics.tnl_outcome_status`
        where Product_usage_duration != 'All' and period_current=cast(CURRENT_DATE() as date)) A
        LEFT join
        (Select * from `{project_name}.tnl_engagement_metrics.tnl_dig_metrics_cat`
        where cast(period_current as date)=cast(CURRENT_DATE() as date)) B
        on A.userid = B.userid
        and cast(A.period_current as date)=cast(B.period_current as date)
        and A.Product_usage_duration=B.Product_usage_duration)
        """.format(project_name=project)
}
#product level view metrics-used needs to append
#    DIG_DASH_PRODVIEW = {
#    'dataset': 'tnl_engagement_metrics',
#    'table_name': 'tnl_engdash_product_view',
#    'sql_query': """
#        (select * from
#        (
#        SELECT period_current, Product_usage_duration,Device,Device_Type,Product,metric_name,metric_value_bin,avg(metric_value) as value
#        FROM `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
#        where cast(period_current as date)= cast(CURRENT_DATE() as date) and metric_name in ('Active Days','Weekday','Weekend','Search Views','Favourite Adds','Favourite Removes','Video Completes','Video Starts','Events','Comments','Weekday_pct','Weekend_pct','Views','Dwell_time','Shares')
#        group by period_current, Product_usage_duration,Device,Device_Type,Product,metric_name,metric_value_bin
#        order by period_current, Product_usage_duration,Device,Device_Type,Product,metric_name,metric_value_bin
#        )
#        UNION ALL
#        (
#        SELECT period_current, Product_usage_duration,Device,Device_Type,Product,'Users' as metric_name,metric_value_bin,count(distinct userid) as value
#        FROM `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
#        where cast(period_current as date)= cast(CURRENT_DATE() as date) and metric_name='Active Days'
#        group by period_current, Product_usage_duration,Device,Device_Type,Product,metric_name,metric_value_bin
#        order by period_current, Product_usage_duration,Device,Device_Type,Product,metric_name,metric_value_bin
#        )
#        UNION ALL
#        (
#        SELECT period_current, Product_usage_duration,Device,Device_Type,Product,'No usage' as metric_name,metric_value_bin,count(distinct userid) as value
#        FROM `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
#        where cast(period_current as date)= cast(CURRENT_DATE() as date) and metric_name='No usage'
#        group by period_current, Product_usage_duration,Device,Device_Type,Product,metric_name,metric_value_bin
#        order by period_current, Product_usage_duration,Device,Device_Type,Product,metric_name,metric_value_bin
#        )
#        order by period_current, Product_usage_duration,Device,Device_Type,Product,metric_name,metric_value_bin)
#        """.format(project_name=project)
#}
#pack level view metrics-used needs to append
#    DIG_DASH_PACKVIEW = {
#    'dataset': 'tnl_engagement_metrics',
#    'table_name': 'tnl_engdash_pack_view',
#    'sql_query': """
#        select * from
#        (
#        SELECT period_current, Product_usage_duration,mpc_current,mpc_type_current,metric_name,metric_value_bin,avg(metric_value) as value
#        FROM `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
#        where cast(period_current as date)=cast(CURRENT_DATE() as date) and metric_name in ('Active Days','Weekday','Weekend','Search Views','Favourite Adds','Favourite Removes','Video Completes','Video Starts','Events','Comments','Weekday_pct','Weekend_pct','Views','Dwell_time','Shares')
#        group by period_current, Product_usage_duration,mpc_current,mpc_type_current,metric_name,metric_value_bin
#        order by period_current, Product_usage_duration,mpc_current,mpc_type_current,metric_name,metric_value_bin
#        )
#        UNION ALL
#        (
#        SELECT period_current, Product_usage_duration,mpc_current,mpc_type_current,'Users' as metric_name,metric_value_bin,count(distinct userid) as value
#        FROM `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
#        where cast(period_current as date)=cast(CURRENT_DATE() as date) and metric_name='Active Days'
#        group by period_current, Product_usage_duration,mpc_current,mpc_type_current,metric_name,metric_value_bin
#        order by period_current, Product_usage_duration,mpc_current,mpc_type_current,metric_name,metric_value_bin
#        )
#        UNION ALL
#        (
#        SELECT period_current, Product_usage_duration,mpc_current,mpc_type_current,'No usage' as metric_name,metric_value_bin,count(distinct userid) as value
#        FROM `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
#        where cast(period_current as date)=cast(CURRENT_DATE() as date) and metric_name='No usage'
#        group by period_current, Product_usage_duration,mpc_current,mpc_type_current,metric_name,metric_value_bin
#        order by period_current, Product_usage_duration,mpc_current,mpc_type_current,metric_name,metric_value_bin
#        )
#        order by period_current, Product_usage_duration,mpc_current,mpc_type_current,metric_name,metric_value_bin)
#        """.format(project_name=project)
#}
#churn rate pack level view to be run needs to append
#    DIG_DASH_PACKCHURN = {
#    'dataset': 'tnl_engagement_metrics',
#    'table_name': 'tnl_pack_type_churn',
#    'sql_query': """
#        SELECT period_current period,Product_usage_duration Product_usage_duration,mpc_type_current mpc_type,metric_name
#        metric_name,metric_value_bin metric_value_bin,ROUND(churners/(obs),3) AS outcome_churn_rate
#        from 
#        (Select period_current,Product_usage_duration,mpc_type_current,metric_name,metric_value_bin,
#        count(distinct userid) churners
#        FROM `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
#        where outcome_churn = 1 and cast(period_current as date)=cast(CURRENT_DATE() as date))
#        group by period_current,Product_usage_duration,mpc_type_current,metric_name,metric_value_bin) A
#        LEFT join
#        (Select period_current,Product_usage_duration,mpc_type_current,metric_name,metric_value_bin,
#        count(distinct userid) obs
#        from `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
#        where cast(period_current as date)=cast(CURRENT_DATE() as date)
#        group by period_current,Product_usage_duration,mpc_type_current,metric_name,metric_value_bin) B
#        ON cast(A.period_current as date)=cast(B.period_current as date)
#        and A.Product_usage_duration=B.Product_usage_duration
#        AND A.mpc_type_current=B.mpc_type_current
#       AND A.metric_name=B.metric_name
#        AND A.metric_value_bin=B.metric_value_bin
#        WHERE A.mpc_type_current IN ('Digital','Digi-Print')
#        """.format(project_name=project)
#}
#churn rate product level view to be run needs to append
 #   DIG_DASH_PRODCHURN = {
 #    'dataset': 'tnl_engagement_metrics',
 #   'table_name': 'tnl_product_churn',
 #   'sql_query': """
 #       SELECT period_current period,Product_usage_duration Product_usage_duration,Product Product,metric_name
 #       metric_name,metric_value_bin metric_value_bin,ROUND(churners/(obs),3) AS outcome_churn_rate
 #      from 
 #       (Select period_current,Product_usage_duration,Product,metric_name,metric_value_bin,
 #       count(distinct userid) churners
 #       FROM `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
 #       where outcome_churn = 1 and cast(period_current as date)=cast(CURRENT_DATE() as date) and mpc_type_current IN ('Digital','Digi-Print')
 #       group by period_current,Product_usage_duration,Product,metric_name,metric_value_bin) A
 #       LEFT join
 #       (Select period_current,Product_usage_duration,Product,metric_name,metric_value_bin,
 #       count(distinct userid) obs
 #       from `{project_name}.tnl_engagement_metrics.tnl_engdash_metrics`
 #       where cast(period_current as date)=cast(CURRENT_DATE() as date) and mpc_type_current IN ('Digital','Digi-Print')
 #       group by period_current,Product_usage_duration,Product,metric_name,metric_value_bin) B
 #       ON cast(A.period_current as date)=cast(B.period_current as date)
 #       and A.Product_usage_duration=B.Product_usage_duration
 #       AND A.Product=B.Product
 #       AND A.metric_name=B.metric_name
 #       AND A.metric_value_bin=B.metric_value_bin
 #       """.format(project_name=project)
#}


    query_list3['DIG_SECTION_USER'] = DIG_SECTION_USER
    query_list3['DIG_SECTION_FINAL'] = DIG_SECTION_FINAL
    query_list3['DIG_1WEEK_SECTION'] = DIG_1WEEK_SECTION
    query_list3['DIG_2WEEK_SECTION'] = DIG_2WEEK_SECTION
    query_list3['DIG_4WEEK_SECTION'] = DIG_4WEEK_SECTION
    query_list3['DIG_12WEEK_SECTION'] = DIG_12WEEK_SECTION
    query_list3['DIG_SECTION_AGG'] = DIG_SECTION_AGG
    query_list3['DIG_METRICS_SECTION'] = DIG_METRICS_SECTION
    query_list3['DIG_SEC_OUTPUT'] = DIG_SEC_OUTPUT
    query_list3['DIG_DASH_METRICS'] = DIG_DASH_METRICS
    ##query_list3['DIG_DASH_PRODVIEW'] = DIG_DASH_PRODVIEW
    ##query_list3['DIG_DASH_PACKVIEW'] = DIG_DASH_PACKVIEW
    ##query_list3['DIG_DASH_PACKCHURN'] = DIG_DASH_PACKCHURN
    ##query_list3['DIG_DASH_PRODCHURN'] = DIG_DASH_PRODCHURN
    return query_list3


def table_exists(client, table_reference):
    """Return if a table exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        table_reference (google.cloud.bigquery.table.TableReference):
            A reference to the table to look for.

    Returns:
        bool: ``True`` if the table exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_table(table_reference)
        return True
    except NotFound:
        return False


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                print "Errors", job.errors
                raise RuntimeError(job.error_result)
            return
        time.sleep(1)


def save_query_to_table(project, query, dataset, table):
    print "Using project: ", project
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = client.dataset(dataset).table(table)
    job_config.destination = table_ref
    job_config.write_disposition="WRITE_TRUNCATE"
    query_job = client.query(query, location='EU',
        job_config=job_config)  # API request - starts the query
    res = query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))

    
def run_export(project, dataset, table_name, sql_query):
    save_query_to_table(project, sql_query, dataset, table_name)


def save_query_to_table2(project, query, dataset, table):
    print "Using project: ", project
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = client.dataset(dataset).table(table)
    job_config.destination = table_ref
    job_config.write_disposition="WRITE_APPEND"
    query_job = client.query(query, location='EU',
        job_config=job_config)  # API request - starts the query
    res = query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))


def run_export2(project, dataset, table_name, sql_query):
    save_query_to_table2(project, sql_query, dataset, table_name)


def export_data_to_gcs(project, dataset_name, table_name, destination):
    bigquery_client = bigquery.Client(project=project)
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    job_name = str(uuid.uuid4())
    job = bigquery_client.extract_table_to_storage(
        job_name, table, destination)
    job.begin()
    wait_for_job(job)
    print('Exported {}:{} to {}'.format(
        dataset_name, table_name, destination))


def run_all(project, dataset_output, output_table1, output_table2, output_table_gcs1,output_table_gcs2):
    print("run_all.dataset_output: ", dataset_output)
    try:
        #project = sys.argv`1`
        print "Using project: ", project
    except IndexError:
        print "Usage: python tnl_engagement_metrics.py GOOGLE_CLOUD_PROJECT"
    else:
        query_list1 = save_query1(project)
        query_list2 = save_query2(project)
        query_list3 = save_query3(project)

        run_export(project, **DIG_USAGE)
        run_export(project, **DIG_USAGE_ALL)
        run_export(project, **DIG_DWELL)
        run_export(project, **DIG_DWELL_ALL)

        run_export(project, **query_list1['DIG_USAGE_OVERALL'])
        run_export(project, **query_list1['DIG_DWELL_OVERALL'])
        run_export(project, **query_list1['DIG_USAGE_MASTER'])
        run_export(project, **query_list1['DIG_1WEEK_USAGE'])
        run_export(project, **query_list1['DIG_2WEEK_USAGE'])
        run_export(project, **query_list1['DIG_4WEEK_USAGE'])
        run_export(project, **query_list1['DIG_12WEEK_USAGE'])
        run_export(project, **query_list1['DIG_METRICS'])
        run_export(project, **query_list1['DIG_METRICS_CAT'])

        run_export(project, **DIG_1WEEK_OUTCOME)
        run_export(project, **DIG_2WEEK_OUTCOME)
        run_export(project, **DIG_4WEEK_OUTCOME)
        run_export(project, **DIG_12WEEK_OUTCOME)
        run_export(project, **DIG_ALL_OUTCOME)

        run_export(project, **query_list2['DIG_OUTCOME_STATUS'])
        run_export2(project, **query_list2['DIG_OUTCOME_LINKED'])
        run_export(project, **query_list2['DIG_TRANSPOSE'])
        run_export(project, **query_list2['DIG_ENG_OUTPUT'])

        run_export(project, **DIG_SECTION_MASTER)

        run_export(project, **query_list3['DIG_SECTION_USER'])
        run_export(project, **query_list3['DIG_SECTION_FINAL'])
        run_export(project, **query_list3['DIG_1WEEK_SECTION'])
        run_export(project, **query_list3['DIG_2WEEK_SECTION'])
        run_export(project, **query_list3['DIG_4WEEK_SECTION'])
        run_export(project, **query_list3['DIG_12WEEK_SECTION'])
        run_export(project, **query_list3['DIG_SECTION_AGG'])
        run_export2(project, **query_list3['DIG_METRICS_SECTION'])
        run_export(project, **query_list3['DIG_SEC_OUTPUT'])
        run_export(project, **query_list3['DIG_DASH_METRICS'])
        ##run_export2(project, **query_list3['DIG_DASH_PRODVIEW'])
        ##run_export2(project, **query_list3['DIG_DASH_PACKVIEW'])
        ##run_export2(project, **query_list3['DIG_DASH_PACKCHURN'])
        ##run_export2(project, **query_list3['DIG_DASH_PRODCHURN'])
        current_time = datetime.now()
        current_date = str(current_time.date())
        #export_data_to_gcs(project, dataset_output, output_table1, output_table_gcs1 % current_date)
        #export_data_to_gcs(project, dataset_output, output_table2, output_table_gcs2 % current_date)

if __name__ == "__main__":
    #run_all()
    run_all(project, dataset_output, output_table1, output_table2, output_table_gcs1,output_table_gcs2)





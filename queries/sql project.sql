#project
Use project;
Select * from orders;

## Task 1:Data cleaning to delete duplicate orderids
Create view new as
(Select Order_ID, Row_Number() Over (Partition BY Order_ID) as rn from orders);
Select * from new;
DELETE FROM orders
WHERE Order_ID IN (
    SELECT Order_ID FROM new WHERE rn > 1
);
SET SQL_SAFE_UPDATES=0;
#Replace null Traffic_Delay_Min with the average delay for that route. 
Select* from routes;
Select Traffic_Delay_Min from routes where Traffic_Delay_Min IS NULL;
SET @avg_delay := (
    SELECT AVG(Traffic_Delay_Min) 
    FROM routes 
    WHERE Traffic_Delay_Min IS NOT NULL
);

UPDATE routes
SET Traffic_Delay_Min = @avg_delay
WHERE Traffic_Delay_Min IS NULL;

Select @avg_delay;
#formatting date 
Select Order_Date, date_format(Order_Date, '%Y-%m-%d') AS Formatteddate from orders;
Select*from orders;
Update orders
SET Order_Date= date_format(Order_Date, '%Y-%m-%d'), Expected_Delivery_Date = date_format(Expected_Delivery_Date, '%Y-%m-%d'),Actual_Delivery_Date = date_format(Actual_Delivery_Date, '%Y-%m-%d');

Select* from deliveryagents;

Select* from shipmenttrackingtable;
Update shipmenttrackingtable 
SET Checkpoint_Time= date_format(Checkpoint_Time, '%Y-%m-%d');

Select * from orders;
#FLAG RECORDS Ensuring that order date is lesser than actual delivery date 
ALTER TABLE orders
ADD COLUMN DATA_FLAG varchar(20);
UPDATE ORDERS 
SET DATA_FLAG=
case
when
Actual_delivery_date> Order_Date then 'Date valid'
else 
'Date invalid'
end;

#Task 2
# delivery delay for each order in days

ALTER TABLE orders
ADD COLUMN Delivery_delay int; 

UPDATE orders
SET Delivery_delay = datediff(Actual_Delivery_Date, Expected_Delivery_Date);

Select* from orders;

#top 10 delayed routes
Select Route_ID, avg(Delivery_delay) as Delivery_Delay_avg from orders group by Route_ID order by Delivery_Delay_avg desc limit 10  ;

#window function rank

Select Warehouse_ID, Order_ID, Delivery_delay, Rank() OVER (partition by Warehouse_ID order by Delivery_delay desc) as Delay_rank from orders;

#Task 3 
#Route optimisation insights

Select* from orders;


#calculate: Average delivery time (in days),Average traffic delay, Distance to time efficiency ratio
Select*from routes;
ALTER TABLE routes
ADD COLUMN Distance_to_time_efficiency_ratio float;
Update routes
SET Distance_to_time_efficiency_ratio = (Distance_KM)/(Average_Travel_Time_Min);
SET SQL_SAFE_UPDATES=0;

SELECT 
    o.Route_ID,
    ROUND(AVG(o.Delivery_delay), 2) AS Avg_Delivery_Time_Days,
    ROUND(AVG(r.Traffic_Delay_Min), 1) AS Avg_Traffic_Delay_Min,
    ROUND(AVG(r.Distance_to_time_efficiency_ratio),1) AS AVG_Distance_to_time_efficiency_ratio
FROM orders o
JOIN routes r 
    ON o.Route_ID = r.Route_ID 
GROUP BY o.Route_ID
ORDER BY Avg_Delivery_Time_Days DESC;


Create view averages as 
SELECT 
    o.Route_ID,
    ROUND(AVG(o.Delivery_delay), 2) AS Avg_Delivery_Time_Days,
    ROUND(AVG(r.Traffic_Delay_Min), 1) AS Avg_Traffic_Delay_Min,
    ROUND(AVG(r.Distance_to_time_efficiency_ratio),1) AS AVG_Distance_to_time_efficiency_ratio
FROM orders o
JOIN routes r 
    ON o.Route_ID = r.Route_ID 
GROUP BY o.Route_ID
ORDER BY Avg_Delivery_Time_Days DESC;

Select* from averages;

#Routes with worst efficiency ratio
Select Route_ID, AVG_Distance_to_time_efficiency_ratio as Routes_with_worst_efficiency_ratio from averages order by AVG_Distance_to_time_efficiency_ratio limit 3;

Select * from shipmenttrackingtable;

Alter table shipmenttrackingtable 
ADD COLUMN Route_ID int;


UPDATE shipmenttrackingtable s
JOIN routes r 
    ON s.Route_ID = r.Route_ID
SET s.Route_ID = r.Route_ID;

Select * from routes;
Create view newtable
as 
Select o. Route_ID, s.Order_ID, s.Shipment_ID,s. Delay_Reason from Shipmenttrackingtable s JOIN orders o ON s.Order_ID= o.Order_ID;


Select* from newtable;
#Find routes with >20% delayed shipments. 
Select Route_ID, count(*) as number_of_shipments, SUM( CASE WHEN Delay_Reason= 'None' THEN 0 ELSE 1 END) AS Number_of_delays, ROUND(SUM( CASE WHEN Delay_Reason= 'None' THEN 0 ELSE 1 END)*100/count(*),2) AS Delay_Percentage from newtable GROUP BY Route_ID HAVING Delay_Percentage> 20;

Create view delays as
Select Route_ID, count(*) as number_of_shipments, SUM( CASE WHEN Delay_Reason= 'None' THEN 0 ELSE 1 END) AS Number_of_delays, ROUND(SUM( CASE WHEN Delay_Reason= 'None' THEN 0 ELSE 1 END)*100/count(*),2) AS Delay_Percentage from newtable GROUP BY Route_ID HAVING Delay_Percentage> 20;


#Recommend potential routes for optimization

Select Route_ID,number_of_shipments, Number_of_delays,Delay_Percentage, dense_rank() OVER (ORDER BY Number_of_delays DESC, Delay_Percentage DESC) AS optimization_priority from delays;

#task 4 
#top 3 warehouse with the highest average processing time

Select*from warehouses;

Select  w.Warehouse_ID, ROUND(avg(w.Processing_Time_Min),0) as Average_Processing_time from orders o JOIN Warehouses w ON o.Warehouse_ID = w.Warehouse_ID GROUP BY Warehouse_ID ORDER BY Average_Processing_time DESC Limit 3;

#total vs delayed shipments for each warehouse


Select SUM( d.number_of_shipments) as Total_shipments, SUM(d.Number_of_delayS) as Delayed_shipments, o.Warehouse_ID from delays d join orders o ON d.Route_ID = O.Route_ID group by Warehouse_ID;

#Use CTEs to find bottleneck warehouses where processing time > global average. 

SET @avg_processing_time= (Select avg(Processing_Time_Min) from warehouses);
Select @avg_processing_time;
WITH bottleneck_warehouses AS
(Select Warehouse_ID as bottleneck_warehouses, Processing_Time_Min from warehouses where Processing_Time_Min > @avg_processing_time)
Select* from bottleneck_warehouses;

#Rank warehouses based on on-time delivery percentage. 
Select* from warehouses;
Select *,Rank() OVER (ORDER BY Ontime_delivery_percentage DESC) as ranking from
(SELECT 
    Warehouse_ID,
    SUM(CASE WHEN Delivery_Status = 'On Time' THEN 1 ELSE 0 END) AS Ontime_deliveries,
    COUNT(*) AS Total_deliveries,
    ROUND(
        SUM(CASE WHEN Delivery_Status = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS Ontime_delivery_percentage
    
FROM orders
GROUP BY Warehouse_ID) AS aggregation;

#Task 5-Delivery agent performance
#Rank agents (per route) by on-time delivery percentage  
Select* from deliveryagents;
Select *, Rank() Over( Order by Average_on_time_delivery_percentage desc) AS Ranking from ( Select Route_ID , avg(On_Time_Percentage) as Average_on_time_delivery_percentage from deliveryagents group by Route_ID) AS aggregation;

#Find agents with on-time % < 80%. 
Select* from deliveryagents;
Select Agent_ID, On_Time_Percentage from deliveryagents where On_Time_Percentage < 80;

#Compare average speed of top 5 vs bottom 5 agents using subqueries. 

Select Agent_ID, Avg_Speed_KM_HR from deliveryagents order by Avg_Speed_KM_HR desc limit 5;
Select Agent_ID, Avg_Speed_KM_HR from deliveryagents order by Avg_Speed_KM_HR asc limit 5;

Select 'Top_5_average_speed' as Category, avg (Avg_Speed_KM_HR) as Top_5_avg from (Select Agent_ID, Avg_Speed_KM_HR from deliveryagents order by Avg_Speed_KM_HR desc limit 5) AS Top_5 UNION ALL Select 'bottom_5_average_speed' as Category, avg(Avg_Speed_KM_HR) as Bottom_5 from (Select Agent_ID, Avg_Speed_KM_HR from deliveryagents order by Avg_Speed_KM_HR asc limit 5) AS Bottom_5;
;

#Task 6, Shipment_tracking_analytics
#For each order, list the last checkpoint and time. 
Select * from Shipmenttrackingtable;

Select Order_ID, max(Checkpoint) as Last_Checkpoint, max(Checkpoint_Time) as Last_Checkpoint_time from shipmenttrackingtable group by Order_ID;

#Find the most common delay reasons (excluding None)

Select max(Delay_Reason) as Most_Common_Delay_reason from shipmenttrackingtable where Delay_Reason NOT IN ('None');

#Identify orders with >2 delayed checkpoints  
SELECT Order_ID, count(Delay_Reason)
FROM shipmenttrackingtable
WHERE Delay_Reason NOT IN ('None')
GROUP BY Order_ID
HAVING COUNT(Delay_Reason) > 2;

#Task 7: Advanced KPI Reporting 

Select*from orders;
Select* from routes;
#Average Delivery Delay per Region (Start_Location). 
Select Round(avg(o.Delivery_delay),2) as Delivery_delay_in_days, r.Start_Location from routes r JOIN orders o ON r.Route_ID = o.Route_ID GROUP BY Start_Location;

#On-Time Delivery % = (Total On-Time Deliveries / Total Deliveries) * 100.

SELECT 
    SUM(CASE WHEN Delivery_Status = 'On Time' THEN 1 ELSE 0 END) AS Ontime_deliveries,
    COUNT(*) AS Total_deliveries,
    ROUND(
        SUM(CASE WHEN Delivery_Status = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS Ontime_delivery_percentage
    FROM orders;

#Average Traffic Delay per Route

Select * from routes;

Select * from orders;

Select Route_ID, Count( Case when Delivery_Status = 'Delayed' then 1 else 0 end ) as Avg_delay_times from orders group by Route_ID;










class ServiceQuery:
    def __init__(self, start_datetime, end_datetime):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    def Get_app_call(self):
        query = f"""
                    WITH code_value AS (
                         SELECT *
                         FROM im_mobility.private_code 
                         WHERE col_name = 'boarding_type'),
                         
                         status_value AS (
                            SELECT *
                            FROM im_mobility.private_code
                            WHERE table_name = 'boarding_history' AND col_name = 'status'),
                        
                        dispatch_type_code AS(
                                SELECT *
                                
                            FROM im_mobility.private_code
                            WHERE `table_name` = 'boarding_history_additional' AND col_name = 'dispatch_type'),
                        
                        payment_type_code AS(		
                            SELECT *
                            FROM im_mobility.private_code
                            WHERE `table_name` = 'boarding_history' AND col_name = 'payment_type'),
                        
                        reservation_type_code AS(
                           SELECT *
                            FROM im_mobility.private_code
                            WHERE `table_name` = 'reservation_boarding_history' AND col_name = 'reservation_type')
                    
                          
                    
                    SELECT a.reg_datetime AS 'reg_datetime',
                                         a.idx AS 'db_idx',
                                       case when (a.is_reservation = 'N' AND a.dispatch_status = 1) then '앱호출'
                                            when (a.is_reservation = 'N' AND a.dispatch_status = 2) then '콜센터'
                                            ELSE '예약호출' END AS 'boarding_name',
                                       code_value.code_value AS 'boarding_type',
                                       dispatch_type_code.code_value AS 'dispatch_type',
                                       payment_type_code.code_value AS 'payment_type',
                                       reservation_type_code.code_value AS 'reservation_type',
                                       status_value.code_value AS 'status',
                                       IFNULL(a.car_idx, 0) AS 'car_idx',
                                       IFNULL(b.car_type_idx, 0) AS 'car_type_idx',
                                       a.dispatch_idx,
                                       a.user_idx,
                                       IFNULL(a.driver_idx, 0) AS 'driver_idx',
                                       IFNULL(a.coupon_gift_idx, 0) AS 'coupon_gift_idx',
                                       CAST(a.fare AS FLOAT) AS 'fare',
                                       CAST(a.toll AS FLOAT) AS 'toll',
                                       CAST(a.additional_amount AS FLOAT) AS 'additional_amount',
                                       CAST(a.discount_amount AS FLOAT) AS 'discount_amount',
                                       CAST(a.use_point AS FLOAT) AS 'use_point',
                                       CAST(IFNULL((SELECT jinmobility_amount
                                                        FROM im_mobility.user_boarding_cancellation_fee_history d
                                                        WHERE d.boarding_history_idx = a.idx
                                                        ORDER BY idx DESC LIMIT 1), 0) AS FLOAT)  AS 'app_cancellation_fee',
                    
                                       CAST(IFNULL((SELECT jinmobility_amount 
                                                        FROM im_mobility.reservation_boarding_payment_history 
                                                        WHERE reservation_boarding_history_idx = c.idx AND amount_type = 2 
                                                        ORDER BY idx DESC LIMIT 1), 0) AS FLOAT) AS 'reservation_cancellation_fee',
                                       IFNULL(a.driving_distance, 0) AS 'driving_distance',
                                       IFNULL(b.obd_driving_distance, 0) AS 'obd_driving_distance',
                                       IFNULL(b.carlog_distance, 0) AS 'carlog_distance',
                                       IFNULL(b.app_meter_distance, 0) AS 'app_meter_distance',
                                       CAST(b.recalculation_fare AS FLOAT) AS 'recalculation_fare',
                                       IFNULL(b.repayment_status, 0) AS 'repayment_status',
                                       CAST(a.estimated_amount AS FLOAT) AS 'estimated_amount',
                                       IFNULL(a.estimated_distance, 0) AS 'estimated_distance',
                                       a.departure_datetime,
                                       a.arrival_datetime,
                                       a.estimated_datetime,
                                       CAST(b.flexible_fare_rate AS FLOAT) AS 'flexible_fare_rate',
                                       CAST(b.priority_fare_rate AS FLOAT) AS 'priority_fare_rate',
                                       CAST(b.magic_chance_discount_rate AS FLOAT) AS 'magic_chance_discount_rate',
                                       CAST(b.pre_fixed_fare AS FLOAT) AS 'pre_fixed_fare',
                                       CAST(a.departure_lat AS FLOAT) AS 'departure_lat',
                                       CAST(a.departure_lng AS FLOAT) AS 'departure_lng',
                                       CAST(a.arrival_lat AS FLOAT) AS 'arrival_lat',
                                       CAST(a.arrival_lng AS FLOAT) AS 'arrival_lng',
                                       a.departure_address,
                                       a.departure_address_detail,
                                       a.arrival_address,
                                       a.arrival_address_detail,
                                       IFNULL(c.reservation_no, '-') AS 'reservation_no',
                                       IFNULL(e.b2b_partners_member_idx, 0) AS 'b2b_partners_member_idx',
                                       f.name AS 'b2b_partners_member_name',
                                       g.name AS 'b2b_company_name',
                                       a.is_call   
                                        
                    FROM im_mobility.boarding_history a LEFT JOIN code_value ON a.boarding_type = code_value.code_name
                                                        LEFT JOIN status_value ON a.`status` = status_value.code_name
                                                        LEFT JOIN im_mobility.boarding_history_additional b ON a.idx = b.boarding_history_idx
                                                        LEFT JOIN dispatch_type_code ON b.dispatch_type = dispatch_type_code.code_name
                                                        LEFT JOIN payment_type_code ON a.payment_type = payment_type_code.code_name
                                                        LEFT JOIN im_mobility.reservation_boarding_history c ON a.idx = c.boarding_history_idx
                                                        LEFT JOIN reservation_type_code ON c.reservation_type = reservation_type_code.code_name
                                                        LEFT JOIN im_mobility.b2b_partners_member_boarding_history e ON a.idx = e.boarding_history_idx
                                                        LEFT JOIN im_mobility.b2b_partners_member f ON e.b2b_partners_member_idx = f.idx
                                                        LEFT JOIN im_mobility.b2b_partners g ON f.b2b_partners_idx = g.idx
                                                        
                                                     
                    WHERE a.reg_datetime BETWEEN '{self.start_datetime}' AND '{self.end_datetime}' 
        AND b.car_type_idx <> 2 AND (a.user_idx NOT IN (SELECT user_idx FROM im_mobility.user WHERE name LIKE '큐에이%') OR a.user_idx IS NULL)"""

        return query

    def Get_general(self):
        query = f"""SELECT a.reg_datetime,
                           a.idx AS 'db_idx',
                           '일반주행' AS 'boarding_name',
                           '개인' AS 'boarding_type',
                           '일반 배차' AS 'dispatch_type',
                           '직접결제' AS 'payment_type',
                           NULL AS 'reservation_type',
                           case when a.`status` = 1 then '주행'
                                when a.`status` = 2 then '하차'
                                ELSE '주행중지' END AS 'status',
                          IFNULL(a.car_idx, 0) AS 'car_idx',
                          1 AS 'car_type_idx',
                          0 AS 'dispatch_idx',
                          0 AS 'user_idx',
                          IFNULL(a.driver_idx, 0) AS 'driver_idx',
                          0 AS 'coupon_gift_idx',
                          CAST(a.fare AS FLOAT) AS 'fare',
                          CAST(a.toll AS FLOAT) AS 'toll',
                          CAST(a.additional_amount AS FLOAT) AS 'additional_amount',
                          CAST(0.0 AS FLOAT) AS 'discount_amount',
                          CAST(0.0 AS FLOAT) AS 'use_point',
                          CAST(0.0 AS FLOAT) AS  'app_cancellation_fee',
                          CAST(0.0 AS FLOAT) AS 'reservation_cancellation_fee',
                          IFNULL(a.driving_distance, 0) AS 'driving_distance',
                          IFNULL(a.obd_driving_distance, 0) AS 'obd_driving_distance',
                          0 AS 'carlog_distance',
                          0 AS 'app_meter_distance',
                          CAST(0.0 AS FLOAT) AS 'recalculation_fare',
                          0 AS 'repayment_status',
                          CAST(0.0 AS FLOAT) AS 'estimated_amount',
                          0 AS 'estimated_distance',
                          a.departure_datetime,
                          a.arrival_datetime,
                          NULL AS 'estimated_datetime',
                          CAST(1.0 AS FLOAT) AS 'flexible_fare_rate',
                          CAST(1.0 AS FLOAT) AS 'priority_fare_rate',
                          CAST(0.0 AS FLOAT) AS 'magic_chance_discount_rate',
                          CAST(0.0 AS FLOAT) AS 'pre_fixed_fare',
                          CAST(a.departure_lat AS FLOAT) AS 'departure_lat',
                          CAST(a.departure_lng AS FLOAT) AS 'departure_lng',
                          CAST(a.arrival_lat AS FLOAT) AS 'arrival_lat',
                          CAST(a.arrival_lng AS FLOAT) AS 'arrival_lng',
                          a.departure_address,
                          a.departure_address_detail,
                          a.arrival_address,
                          a.arrival_address_detail,
                          '-' AS 'reservation_no',
                            0 AS 'b2b_partners_member_idx',
                            NULL AS 'b2b_partners_member_name',
                            NULL AS 'b2b_company_name',
                            'N' AS 'is_call'
                           
                    FROM im_mobility.general_boarding_history a
                    WHERE a.reg_datetime BETWEEN '{self.start_datetime}' AND '{self.end_datetime}' """
        return query

    def Get_reservation_cancel(self):
        query = f"""
                    WITH code_value AS (
                         SELECT *
                         FROM im_mobility.private_code 
                         WHERE col_name = 'boarding_type'),
                    
                        payment_type_code AS(		
                            SELECT *
                            FROM im_mobility.private_code
                            WHERE `table_name` = 'boarding_history' AND col_name = 'payment_type'),
                        
                        reservation_type_code AS(
                           SELECT *
                            FROM im_mobility.private_code
                            WHERE `table_name` = 'reservation_boarding_history' AND col_name = 'reservation_type'),
                        
                        reservation_status_code AS(
                               SELECT *
                                FROM im_mobility.private_code
                                WHERE table_name = 'reservation_boarding_history' AND col_name = 'reservation_status')

                    SELECT a.reg_datetime,
                             a.idx AS 'db_idx',
                             '예약호출' AS 'boarding_name',
                             code_value.code_value AS 'boarding_type',
                             '일반 배차' AS 'dispatch_type',
                             payment_type_code.code_value AS 'payment_type',
                             reservation_type_code.code_value AS 'reservation_type',
                             reservation_status_code.code_value AS 'status',
                             0 AS 'car_idx',
                             IFNULL(a.car_type_idx, 0) AS 'car_type_idx',
                             0 AS 'dispatch_idx',
                             a.user_idx,
                             IFNULL(a.driver_idx, 0) AS 'driver_idx',
                           IFNULL(a.coupon_gift_idx, 0) AS 'coupon_gift_idx',
                             CAST(a.fare AS FLOAT) AS 'fare',
                             CAST(a.toll AS FLOAT) AS 'toll',
                             CAST(a.additional_amount AS FLOAT) AS 'additional_amount',
                             CAST(a.discount_amount AS FLOAT) AS 'discount_amount',
                             CAST(a.use_point AS FLOAT) AS 'use_point',
                             CAST(0.0 AS FLOAT) AS 'app_cancellation_fee',
		                     CAST(IFNULL((SELECT jinmobility_amount 
                                                FROM im_mobility.reservation_boarding_payment_history 
                                                WHERE reservation_boarding_history_idx = a.idx AND amount_type = 2 
                                                ORDER BY idx DESC LIMIT 1), 0) AS FLOAT)AS 'reservation_cancellation_fee',
                             0 AS 'driving_distance',
                             0 AS 'obd_driving_distance',
                             0 AS 'carlog_distance',
                             0 AS 'app_meter_distance',
                             CAST(0.0 AS FLOAT) AS 'recalculation_fare',
                             0 AS 'repayment_status',
                             CAST(a.estimated_fare AS FLOAT) AS 'estimated_amount',
                             IFNULL(a.estimated_distance, 0) AS 'estimated_distance',
                             a.reservation_datetime AS 'departure_datetime',
                             NULL AS 'arrival_datetime',
                             NULL AS 'estimated_datetime',
                             CAST(a.flexible_fare_rate AS FLOAT) AS 'flexible_fare_rate',
                             CAST(1.0 AS FLOAT) AS 'priority_fare_rate',
                             CAST(0.0 AS FLOAT) AS 'magic_chance_discount_rate',
                             CAST(0.0 AS FLOAT) AS  'pre_fixed_fare',
                           CAST(a.departure_lat AS FLOAT) AS 'departure_lat',
                           CAST(a.departure_lng AS FLOAT) AS 'departure_lng',
                           CAST(a.arrival_lat AS FLOAT) AS 'arrival_lat',
                           CAST(a.arrival_lng AS FLOAT) AS 'arrival_lng',
                             a.departure_address,
                             a.departure_address_detail,
                             a.arrival_address,
                             a.arrival_address_detail,
                             IFNULL(a.reservation_no, '-') AS 'reservation_no',
                             0 'b2b_partners_member_idx',
                             NULL AS 'b2b_partners_member_name',
                             b.name AS 'b2b_company_name',
                             a.is_call
                             
                    
                    FROM im_mobility.reservation_boarding_history a LEFT JOIN code_value ON a.boarding_type = code_value.code_name
                                                                    LEFT JOIN payment_type_code ON a.payment_type = payment_type_code.code_name
                                                                    LEFT JOIN reservation_type_code ON a.reservation_type = reservation_type_code.code_name
                                                                    LEFT JOIN im_mobility.b2b_partners b ON a.b2b_partners_idx = b.idx
                                                                    LEFT JOIN reservation_status_code ON a.reservation_status = reservation_status_code.code_name 
                    
                    WHERE a.is_cancel = 'Y' AND a.boarding_history_idx IS NULL 
                         AND a.reg_datetime BETWEEN '{self.start_datetime}' AND '{self.end_datetime}' 
                         AND (a.user_idx NOT IN (SELECT user_idx FROM im_mobility.user WHERE name LIKE '큐에이%') OR a.user_idx IS NULL) """
        return query

class UpdateQuery:
    def __init__(self, start_datetime, end_datetime):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    def update_app_call(self):
        query = f"""
                WITH code_value AS (
                     SELECT *
                     FROM im_mobility.private_code 
                     WHERE col_name = 'boarding_type'),
                     
                     status_value AS (
                        SELECT *
                        FROM im_mobility.private_code
                        WHERE table_name = 'boarding_history' AND col_name = 'status'),
                    
                    dispatch_type_code AS(
                            SELECT *
                            
                        FROM im_mobility.private_code
                        WHERE `table_name` = 'boarding_history_additional' AND col_name = 'dispatch_type'),
                    
                    payment_type_code AS(		
                        SELECT *
                        FROM im_mobility.private_code
                        WHERE `table_name` = 'boarding_history' AND col_name = 'payment_type'),
                    
                    reservation_type_code AS(
                       SELECT *
                        FROM im_mobility.private_code
                        WHERE `table_name` = 'reservation_boarding_history' AND col_name = 'reservation_type')

                SELECT a.reg_datetime AS 'reg_datetime',
                                     a.idx AS 'db_idx',
                                   case when (a.is_reservation = 'N' AND a.dispatch_status = 1) then '앱호출'
                                        when (a.is_reservation = 'N' AND a.dispatch_status = 2) then '콜센터'
                                        ELSE '예약호출' END AS 'boarding_name',
                                   code_value.code_value AS 'boarding_type',
                                   dispatch_type_code.code_value AS 'dispatch_type',
                                   payment_type_code.code_value AS 'payment_type',
                                   reservation_type_code.code_value AS 'reservation_type',
                                   status_value.code_value AS 'status',
                                   IFNULL(a.car_idx, 0) AS 'car_idx',
                                   IFNULL(b.car_type_idx, 0) AS 'car_type_idx',
                                   a.dispatch_idx,
                                   a.user_idx,
                                   IFNULL(a.driver_idx, 0) AS 'driver_idx',
                                   IFNULL(a.coupon_gift_idx, 0) AS 'coupon_gift_idx',
                                   CAST(a.fare AS FLOAT) AS 'fare',
                                   CAST(a.toll AS FLOAT) AS 'toll',
                                   CAST(a.additional_amount AS FLOAT) AS 'additional_amount',
                                   CAST(a.discount_amount AS FLOAT) AS 'discount_amount',
                                   CAST(a.use_point AS FLOAT) AS 'use_point',
                                   CAST(IFNULL((SELECT jinmobility_amount
                                                    FROM im_mobility.user_boarding_cancellation_fee_history d
                                                    WHERE d.boarding_history_idx = a.idx
                                                    ORDER BY idx DESC LIMIT 1), 0) AS FLOAT)  AS 'app_cancellation_fee',
                
                                   CAST(IFNULL((SELECT jinmobility_amount 
                                                    FROM im_mobility.reservation_boarding_payment_history 
                                                    WHERE reservation_boarding_history_idx = c.idx AND amount_type = 2 
                                                    ORDER BY idx DESC LIMIT 1), 0) AS FLOAT) AS 'reservation_cancellation_fee',
                                   IFNULL(a.driving_distance, 0) AS 'driving_distance',
                                   IFNULL(b.obd_driving_distance, 0) AS 'obd_driving_distance',
                                   IFNULL(b.carlog_distance, 0) AS 'carlog_distance',
                                   IFNULL(b.app_meter_distance, 0) AS 'app_meter_distance',
                                   CAST(b.recalculation_fare AS FLOAT) AS 'recalculation_fare',
                                   IFNULL(b.repayment_status, 0) AS 'repayment_status',
                                   CAST(a.estimated_amount AS FLOAT) AS 'estimated_amount',
                                   IFNULL(a.estimated_distance, 0) AS 'estimated_distance',
                                   a.departure_datetime,
                                   a.arrival_datetime,
                                   a.estimated_datetime,
                                   CAST(b.flexible_fare_rate AS FLOAT) AS 'flexible_fare_rate',
                                   CAST(b.priority_fare_rate AS FLOAT) AS 'priority_fare_rate',
                                   CAST(b.magic_chance_discount_rate AS FLOAT) AS 'magic_chance_discount_rate',
                                   CAST(b.pre_fixed_fare AS FLOAT) AS 'pre_fixed_fare',
                                   CAST(a.departure_lat AS FLOAT) AS 'departure_lat',
                                   CAST(a.departure_lng AS FLOAT) AS 'departure_lng',
                                   CAST(a.arrival_lat AS FLOAT) AS 'arrival_lat',
                                   CAST(a.arrival_lng AS FLOAT) AS 'arrival_lng',
                                   a.departure_address,
                                   a.departure_address_detail,
                                   a.arrival_address,
                                   a.arrival_address_detail,
                                   IFNULL(c.reservation_no, '-') AS 'reservation_no',
                                   IFNULL(e.b2b_partners_member_idx, 0) AS 'b2b_partners_member_idx',
                                   f.name AS 'b2b_partners_member_name',
                                   g.name AS 'b2b_company_name',
                                   a.is_call   
                 
                                    
                FROM im_mobility.boarding_history a LEFT JOIN code_value ON a.boarding_type = code_value.code_name
                                                    LEFT JOIN status_value ON a.`status` = status_value.code_name
                                                    LEFT JOIN im_mobility.boarding_history_additional b ON a.idx = b.boarding_history_idx
                                                    LEFT JOIN dispatch_type_code ON b.dispatch_type = dispatch_type_code.code_name
                                                    LEFT JOIN payment_type_code ON a.payment_type = payment_type_code.code_name
                                                    LEFT JOIN im_mobility.reservation_boarding_history c ON a.idx = c.boarding_history_idx
                                                    LEFT JOIN reservation_type_code ON c.reservation_type = reservation_type_code.code_name
                                                    LEFT JOIN im_mobility.b2b_partners_member_boarding_history e ON a.idx = e.boarding_history_idx
                                                    LEFT JOIN im_mobility.b2b_partners_member f ON e.b2b_partners_member_idx = f.idx
                                                    LEFT JOIN im_mobility.b2b_partners g ON f.b2b_partners_idx = g.idx
                                                    
                WHERE a.update_datetime BETWEEN '{self.start_datetime}' AND '{self.end_datetime}' 
                        AND (a.user_idx NOT IN (SELECT user_idx FROM im_mobility.user WHERE name LIKE '큐에이%') OR a.user_idx IS NULL)
                      AND a.reg_datetime < '{self.start_datetime}' AND  b.car_type_idx <> 2 """

        return query

    def update_general(self):
        query = f"""SELECT a.reg_datetime,
                           a.idx AS 'db_idx',
                           '일반주행' AS 'boarding_name',
                           '개인' AS 'boarding_type',
                           '일반 배차' AS 'dispatch_type',
                           '직접결제' AS 'payment_type',
                           NULL AS 'reservation_type',
                           case when a.`status` = 1 then '주행'
                                when a.`status` = 2 then '하차'
                                ELSE '주행중지' END AS 'status',
                          IFNULL(a.car_idx, 0) AS 'car_idx',
                          1 AS 'car_type_idx',
                          0 AS 'dispatch_idx',
                          0 AS 'user_idx',
                          IFNULL(a.driver_idx, 0) AS 'driver_idx',
                          0 AS 'coupon_gift_idx',
                          CAST(a.fare AS FLOAT) AS 'fare',
                          CAST(a.toll AS FLOAT) AS 'toll',
                          CAST(a.additional_amount AS FLOAT) AS 'additional_amount',
                          CAST(0.0 AS FLOAT) AS 'discount_amount',
                          CAST(0.0 AS FLOAT) AS 'use_point',
                          CAST(0.0 AS FLOAT) AS  'app_cancellation_fee',
                          CAST(0.0 AS FLOAT) AS 'reservation_cancellation_fee',
                          IFNULL(a.driving_distance, 0) AS 'driving_distance',
                          IFNULL(a.obd_driving_distance, 0) AS 'obd_driving_distance',
                          0 AS 'carlog_distance',
                          0 AS 'app_meter_distance',
                          CAST(0.0 AS FLOAT) AS 'recalculation_fare',
                          0 AS 'repayment_status',
                          CAST(0.0 AS FLOAT) AS 'estimated_amount',
                          0 AS 'estimated_distance',
                          a.departure_datetime,
                          a.arrival_datetime,
                          NULL AS 'estimated_datetime',
                          CAST(1.0 AS FLOAT) AS 'flexible_fare_rate',
                          CAST(1.0 AS FLOAT) AS 'priority_fare_rate',
                          CAST(0.0 AS FLOAT) AS 'magic_chance_discount_rate',
                          CAST(0.0 AS FLOAT) AS 'pre_fixed_fare',
                          CAST(a.departure_lat AS FLOAT) AS 'departure_lat',
                          CAST(a.departure_lng AS FLOAT) AS 'departure_lng',
                          CAST(a.arrival_lat AS FLOAT) AS 'arrival_lat',
                          CAST(a.arrival_lng AS FLOAT) AS 'arrival_lng',
                          a.departure_address,
                          a.departure_address_detail,
                          a.arrival_address,
                          a.arrival_address_detail,
                          '-' AS 'reservation_no',
                            0 AS 'b2b_partners_member_idx',
                            NULL AS 'b2b_partners_member_name',
                            NULL AS 'b2b_company_name',
                            'N' AS 'is_call'
                           
                    FROM im_mobility.general_boarding_history a
                    WHERE ((a.arrival_datetime BETWEEN '{self.start_datetime}' AND '{self.end_datetime}') OR 
                          (a.cancel_datetime BETWEEN '{self.start_datetime}' AND '{self.end_datetime}')) 
		                    AND a.reg_datetime < '{self.start_datetime}'
                 """
        return query

    def update_reservation_cancel(self):
        query = f"""
                    WITH code_value AS (
                         SELECT *
                         FROM im_mobility.private_code 
                         WHERE col_name = 'boarding_type'),
                    
                        payment_type_code AS(		
                            SELECT *
                            FROM im_mobility.private_code
                            WHERE `table_name` = 'boarding_history' AND col_name = 'payment_type'),
                        
                        reservation_type_code AS(
                           SELECT *
                            FROM im_mobility.private_code
                            WHERE `table_name` = 'reservation_boarding_history' AND col_name = 'reservation_type'),
                            
                        reservation_status_code AS(
                           SELECT *
                            FROM im_mobility.private_code
                            WHERE table_name = 'reservation_boarding_history' AND col_name = 'reservation_status')

                    SELECT a.reg_datetime,
                             a.idx AS 'db_idx',
                             '예약호출' AS 'boarding_name',
                             code_value.code_value AS 'boarding_type',
                             '일반 배차' AS 'dispatch_type',
                             payment_type_code.code_value AS 'payment_type',
                             reservation_type_code.code_value AS 'reservation_type',
                             reservation_status_code.code_value AS 'status',
                             0 AS 'car_idx',
                             IFNULL(a.car_type_idx, 0) AS 'car_type_idx',
                             0 AS 'dispatch_idx',
                             a.user_idx,
                             IFNULL(a.driver_idx, 0) AS 'driver_idx',
                           IFNULL(a.coupon_gift_idx, 0) AS 'coupon_gift_idx',
                             CAST(a.fare AS FLOAT) AS 'fare',
                             CAST(a.toll AS FLOAT) AS 'toll',
                             CAST(a.additional_amount AS FLOAT) AS 'additional_amount',
                             CAST(a.discount_amount AS FLOAT) AS 'discount_amount',
                             CAST(a.use_point AS FLOAT) AS 'use_point',
                             CAST(0.0 AS FLOAT) AS 'app_cancellation_fee',
                             CAST(IFNULL((SELECT jinmobility_amount 
                                                        FROM im_mobility.reservation_boarding_payment_history 
                                                        WHERE reservation_boarding_history_idx = a.idx AND amount_type = 2 
                                                        ORDER BY idx DESC LIMIT 1), 0) AS FLOAT)AS 'reservation_cancellation_fee',
                             0 AS 'driving_distance',
                             0 AS 'obd_driving_distance',
                             0 AS 'carlog_distance',
                             0 AS 'app_meter_distance',
                             CAST(0.0 AS FLOAT) AS 'recalculation_fare',
                             0 AS 'repayment_status',
                             CAST(a.estimated_fare AS FLOAT) AS 'estimated_amount',
                             IFNULL(a.estimated_distance, 0) AS 'estimated_distance',
                             a.reservation_datetime AS 'departure_datetime',
                             NULL AS 'arrival_datetime',
                             NULL AS 'estimated_datetime',
                             CAST(a.flexible_fare_rate AS FLOAT) AS 'flexible_fare_rate',
                             CAST(1.0 AS FLOAT) AS 'priority_fare_rate',
                             CAST(0.0 AS FLOAT) AS 'magic_chance_discount_rate',
                             CAST(0.0 AS FLOAT) AS  'pre_fixed_fare',
                           CAST(a.departure_lat AS FLOAT) AS 'departure_lat',
                           CAST(a.departure_lng AS FLOAT) AS 'departure_lng',
                           CAST(a.arrival_lat AS FLOAT) AS 'arrival_lat',
                           CAST(a.arrival_lng AS FLOAT) AS 'arrival_lng',
                             a.departure_address,
                             a.departure_address_detail,
                             a.arrival_address,
                             a.arrival_address_detail,
                             IFNULL(a.reservation_no, '-') AS 'reservation_no',
                             0 'b2b_partners_member_idx',
                             NULL AS 'b2b_partners_member_name',
                             b.name AS 'b2b_company_name',
                             a.is_call
                             
                    
                    FROM im_mobility.reservation_boarding_history a LEFT JOIN code_value ON a.boarding_type = code_value.code_name
                                                                    LEFT JOIN payment_type_code ON a.payment_type = payment_type_code.code_name
                                                                    LEFT JOIN reservation_type_code ON a.reservation_type = reservation_type_code.code_name
                                                                    LEFT JOIN im_mobility.b2b_partners b ON a.b2b_partners_idx = b.idx 
                                                                    LEFT JOIN reservation_status_code ON a.reservation_status = reservation_status_code.code_name
                    
                    WHERE a.is_cancel = 'Y' AND a.boarding_history_idx IS NULL 
                          AND a.update_datetime BETWEEN '{self.start_datetime}' AND '{self.end_datetime}'  
                          AND (a.user_idx NOT IN (SELECT user_idx FROM im_mobility.user WHERE name LIKE '큐에이%') OR a.user_idx IS NULL) 
                          AND a.reg_datetime < '{self.start_datetime}'  
                 """
        return query

class DeleteBigQuery():
    def __init__(self, table_id, update_start_datetime, to_datetime):
        self.table_id = table_id
        self.update_start_datetime = update_start_datetime
        self.to_datetime = to_datetime

    def delete_data(self, db_idx_tuple: tuple, boarding_name: str):

        if len(db_idx_tuple) == 1:
            _db_idx = db_idx_tuple[0]
            query = f"""DELETE
                        FROM  {self.table_id}
                        WHERE db_idx = {_db_idx} 
                              AND boarding_name = '{boarding_name}' 
                              AND reg_datetime >='{self.update_start_datetime}' AND 
                                  reg_datetime < '{self.to_datetime}' """
        else:
            query = f"""DELETE
                        FROM  {self.table_id}
                        WHERE db_idx in {db_idx_tuple} 
                              AND boarding_name = '{boarding_name}' 
                              AND reg_datetime >='{self.update_start_datetime}' AND 
                                  reg_datetime < '{self.to_datetime}' """
        return query


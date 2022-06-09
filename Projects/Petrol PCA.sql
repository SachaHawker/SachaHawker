
-- how many Target / control
Select segment, count(distinct ec_ID) from "CUSTOMER_ANALYTICS"."TD_REPORTING"."TD02_2223_PETROL_DM" where target_control_flag = '0' group by segment;

-- TD08 Target: 267920 (Inactive:33487 Infrequent:183793 Lapsed:50640) Control: 14071 (Inactive:1731 Infrequent:9717 Lapsed:2623)
-- TD09 Target:  (Inactive:75966 Infrequent:300325 Lapsed:100349) Control: (Inactive:4072 Infrequent:15787 Lapsed:5269)
-- TD10 Target:  (Inactive:33537 Infrequent:184001 Lapsed:50842) Control: (Inactive:1763 Infrequent:9786 Lapsed: 2574)
-- TD11 Target:  (Inactive:33753 Infrequent:185240 Lapsed:51050) Control: (Inactive:1773 Infrequent:9807 Lapsed:2690)
-- TD01 Target:  (Inactive:46359 Infrequent:162090  Lapsed:60857) Control: (Inactive:5032 Infrequent:18183 Lapsed:6716)
-- TD02 Target:  (Inactive:31456 Infrequent: 183141 Lapsed:49524) Control: (Inactive:3474 Infrequent:20477 Lapsed:5385)
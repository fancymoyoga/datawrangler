
Appointments
- Appointment_ID (int)
- Provider_ID (int)
- Patient_ID (int)
- Start_Time (datetime)
- End_Time (datetime)
- Status (varchar) -- Completed, Cancelled, NoShow
 
 Telehealth_Log Table -- (Only Telehealth appts will exist in this table. Not all appointments have these logs. )
- log_ID (int)
- Appointment_ID (int)
- Join_Time (datetime)
- Exit_Time (datetime)

 SELECT Patient_ID, Provider_ID, Join_Time, Exit_Time
 FROM Appointments app
 LEFT  JOIN Telehealth_Log tele on tele.Appointment_ID = =app.Appointment_ID
 Where 1=1
 and Status ='Completed'
 and Start_Time <= DATEADD(CURRENT_DATE(),-30, day)
 -- Task 1:
-- Write a query to find Provider_IDs and Patient_IDs for completed appointments in the last 30 day

provider_id
provider_status (active, termed)
last_update_date

function upsert_provider(existing_data, update_data) 
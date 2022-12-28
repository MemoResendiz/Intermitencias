SELECT t1.cuenta, t1.sum_megas_subida, t1.sum_megas_bajada, t1.sum_segundos, t1.numero_fallas,
       t2.cambio_ip
FROM(SELECT cuenta, SUM(megas_subida) AS sum_megas_subida, SUM(megas_bajada) AS sum_megas_bajada, SUM(segundo_transcurrido) AS sum_segundos,
       SUM(CASE WHEN segundo_transcurrido<900 THEN 1
                ELSE 0
           END) AS numero_fallas
     FROM(SELECT cuenta, productid, bytes_subida/1000000 AS megas_subida, 
     		bytes_bajada/1000000 AS megas_bajada, begin_completa, end_completa, segundo_transcurrido, semana
          FROM(select cuenta,productid,
                      bytes_subida, bytes_bajada,
                      to_timestamp (begintime, 'YYYYMMDDHH24MISS',true) as begin_completa,
                      to_char(begin_completa,'HH24:MI:SS') as begin_hora,
                      to_timestamp (endtime, 'YYYYMMDDHH24MISS',true) as end_completa,
                      to_char(end_completa,'HH24:MI:SS') as end_hora,
                      DATEDIFF(sec,begin_completa::timestamp,end_completa::timestamp)::float as segundo_transcurrido,
                      substring(begintime,9,2) as hora,
                      ip_cliente,
                      (substring(productid,2,4)*0.2)::float  as Subida_paquete,
                      substring(productid,2,4)::float as Bajada_paquete,
                      (Subida_paquete * segundo_transcurrido)  as total_megas_subida,
                      (Bajada_paquete * segundo_transcurrido)  as total_megas_bajada,
                      EXTRACT(DAYOFWEEK FROM begin_completa) AS dia_de_la_semana,
                      EXTRACT(WEEK FROM begin_completa) AS semana
                      
                     from data_lake.edr_aaa
              where info_day between 20221101 and 20221130
                     and substring(begintime,1,6)::integer = 202211
                     --and begintime=endtime
                     and cuenta='0100014525'
                     )
          GROUP BY cuenta, productid, bytes_subida, bytes_bajada, begin_completa, end_completa, segundo_transcurrido, semana)
     GROUP BY cuenta) AS t1
JOIN(
     SELECT cuenta, COUNT(ip_cliente) AS cambio_ip
     FROM(SELECT cuenta, ip_cliente
          FROM(select cuenta,productid,
                      bytes_subida, bytes_bajada,
                      to_timestamp (begintime, 'YYYYMMDDHH24MISS',true) as begin_completa,
                      to_char(begin_completa,'HH24:MI:SS') as begin_hora,
                      to_timestamp (endtime, 'YYYYMMDDHH24MISS',true) as end_completa,
                      to_char(end_completa,'HH24:MI:SS') as end_hora,
                      DATEDIFF(sec,begin_completa::timestamp,end_completa::timestamp)::float as segundo_transcurrido,
                      substring(begintime,9,2) as hora,
                      ip_cliente,
                      (substring(productid,2,4)*0.2)::float  as Subida_paquete,
                      substring(productid,2,4)::float as Bajada_paquete,
                      (Subida_paquete * segundo_transcurrido)  as total_megas_subida,
                      (Bajada_paquete * segundo_transcurrido)  as total_megas_bajada,
                      EXTRACT(DAYOFWEEK FROM begin_completa) AS dia_de_la_semana,
                      EXTRACT(WEEK FROM begin_completa) AS semana
                     
               from data_lake.edr_aaa
              --where info_day in( 20221101, 20221102 )
               where info_day --between 20221101 and 20221130
                     and substring(begintime,1,6)::integer = 202211
                     --and begintime=endtime
                     and cuenta='0100014525')
	                     
	          GROUP BY cuenta, ip_cliente)
     GROUP BY cuenta) AS t2
ON t1.cuenta=t2.cuenta

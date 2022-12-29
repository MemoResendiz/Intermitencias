create table public.cancelado_dataset_delim as (

select 
distinct(delim.cuenta),
delim.status,
delim.created_t,
delim.vida_util,
delim.anio_created,
delim.mes_created,
delim.status_cuenta,
delim.cancel_t,
delim.anio_cancel,
delim.mes_cancel,

modems.model_ont,
modems.p_model_ont,
modems.version_fm,
modems.vendor_ont,
intermitencia.num_intermitencia,

geografia.zona,
geografia.desc_servicio,
geografia.cluster,
geografia.distrito,
geografia.ciudad,
geografia.plaza,
geografia.region,
geografia.tipo_bundle,
geografia.familia,
geografia.fam_num,
estados.d_codigo,
estados.d_mnpio,
estados.d_estado,
estados.indicador

from (
select 
account_no as cuenta,
status,created_t,
vida_util,anio_created,
mes_created,status_cuenta,cancel_t,
anio_cancel,mes_cancel from (

SELECT *, case
       when status='10100' or status='10102' then 0
       else (CAST(SUBSTRING(cancel_t,1,4) as int))
       end as anio_cancel, case
       when status='10100' or status='10102' then 0
       else (CAST(SUBSTRING(cancel_t,6,2) as int))       
       end as mes_cancel FROM(
SELECT account_no, status,created_t,vida_util, (CAST(SUBSTRING(created_t,1,4) as int)) as anio_created, (CAST(SUBSTRING(created_t,6,2) as int)) as mes_created, status_cuenta,
 (DATEADD(month, vida_util ,date(created_t))) AS cancel_t FROM(
  select 
     account_no,
     status,created_t,
     case when status = '10103' then DATEDIFF(MM , date(created_t), date(from_unixtime(last_status_t)))
           when status='10102'  then DATEDIFF(MM , date(created_t), CURRENT_DATE ) 
           when status='10100' then DATEDIFF(MM , date(created_t), CURRENT_DATE ) 
      end as vida_util,
    
      case when status='10100'
        then 'Activo'
        When status='10102'
        then 'Inactivo'
        when status='10103'
        then 'Cancelada'
  end as status_cuenta
  from data_staging.brm_account_t where info_day= 20221226)
  
WHERE SUBSTRING(account_no,1,2) = '1.' OR SUBSTRING(account_no,1,3) IN ('010','011'))

) 
where anio_cancel = 2022) as delim

left join (SELECT b.cuenta, b.model_ont, b.p_model_ont, b.version_fm, b.vendor_ont
           FROM(SELECT cuenta, MAX(info_day) AS ultima_fecha
                FROM bi.cta_model_ont
                GROUP BY cuenta) AS a
           JOIN(SELECT cuenta,
                       model_ont,
                       CASE WHEN model_ont IN ('HG8045H','HG8247','HG8245','ZTEG-824X') THEN 'antiguo'
                            WHEN model_ont IN ('HG8245H','ZTEG-F660','FH-AN55060','AN5506-04-F') THEN 'seminuevo'
                            WHEN model_ont IN ('HG8145V5','F660V7.0','F670LV9.0','F670LV9.0B','HG6145F','ZTEG-F670','HG8145X6') THEN 'nuevo'
                       END AS p_model_ont,
                       version_fm,
                       vendor_ont,
                       info_day
                  FROM bi.cta_model_ont
                  WHERE p_model_ont is not null) AS b
            ON a.cuenta=b.cuenta
            WHERE a.ultima_fecha=b.info_day) as modems

on delim.cuenta=modems.cuenta
 left join (
select 
--count(*) as num_intermitencia
*
       from ( select cuenta,productid,
             to_timestamp (begintime, 'YYYYMMDDHH24MISS',true) as begin_completa,
             to_char(begin_completa,'HH24:MI:SS') as begin_hora,
             to_timestamp (endtime, 'YYYYMMDDHH24MISS',true) as end_completa,
             to_char(end_completa,'HH24:MI:SS') as end_hora,
             DATEDIFF(sec,begin_completa::timestamp,end_completa::timestamp) as segundo_transcurrido,
             substring(begintime,9,2) as hora,
             (substring(productid,2,4)*0.1)::float  as Subida_paquete,
            substring(productid,2,4)::float as Bajada_paquete,
            (Subida_paquete * segundo_transcurrido)  as total_megas_subida,
            (Bajada_paquete * segundo_transcurrido)  as total_megas_bajada,
            CASE when segundo_transcurrido >= 900  END as sesiones_completas,
            CASE when   segundo_transcurrido < 899 END as  sesiones_incompletas,
            ---when segundo_transcurrido >=1 and segundo_transcurrido < 898 then 'interrupcion'
            CASE when segundo_transcurrido = 0  as interrupciones
                                  from data_lake.edr_aaa
         --where info_day between 20221001 and 20221031
         where info_day=20221009
               --and substring(begintime,1,6)::integer = 202210
                and begintime=endtime
                
               and cuenta = '0113218431'
               )
               group by cuenta ) intermitencia
on delim.cuenta=intermitencia.cuenta 

left join (
SELECT cuenta,
    A.zona,
    A.desc_servicio,
    cluster,
    distrito,
    ciudad,
    plaza,
    region, 
    tipo_bundle,
    familia, 
    case when familia='TOTALPLAY INTERNET MAS TELEVISION' then 3 else 2 end as fam_num 
    FROM 
    (select cuenta, zona, desc_servicio from black_box.cartera where info_day=20221031) A 
    LEFT JOIN 
    (SELECT "CLUSTER BRM" AS ZONA,
    "CLUSTER HOMOLOGADO" AS CLUSTER,
    distrito,
    plaza,ciudad,
    "región" as region 
    FROM bi.ds_cobertura where partition_0=202210) B 
    ON A.ZONA=B.ZONA 
    LEFT JOIN 
    (SELECT PLAN, "TIPO DE BUNDLE" AS tipo_bundle, familia FROM bi.ds_planes_homologados where partition_0=202210 )C 
    ON A.desc_servicio=C.plan) as geografia 
on delim.cuenta = geografia.cuenta

left join (
select 
idcuentabrm__c,
geolocalizacioninstalacion__latitude__s,
geolocalizacioninstalacion__longitude__s,
planservicio__c,
leadsource__c
from data_staging.slf_cuentafactura__c  where info_day= 20221030) as geo_coord
on  modems.cuenta=geo_coord.idcuentabrm__c
-- estado --
left join (
SELECT cf.idcuentabrm__c, s.d_codigo,s.d_mnpio, s.d_estado, i.indicador, i.mes
FROM nuevos_negocios.sepomex AS s 
     JOIN data_staging.slf_cuentafactura__c AS cf
     ON s.d_codigo=cf.codigopostalinstalacion__c
     JOIN (SELECT nom_ent, indicador, mes
           FROM data_lake.cat_inflacion_estado
           WHERE mes=10) AS i
     ON s.d_estado=i.nom_ent
WHERE cf.info_day = 20221220
) as estados
on  modems.cuenta= estados.idcuentabrm__c
);
commit;

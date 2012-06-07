--
-- Llista les línies de baixa per poder validar F5
--
select e.id,e.num_linia,e.longitud_cad,tl.name,tc.name,baixa,cini 
from giscedata_bt_element e 
left join giscedata_bt_tipuslinia tl ON (tl.id=e.tipus_linia) 
LEFT JOIN giscedata_bt_cables c ON (c.id=e.cable) 
LEFT JOIN giscedata_bt_tipuscable tc ON (tc.id=c.tipus) 
UNION 
select 0, 'total' , SUM(longitud_cad),'TOTAL','TOTAL',True,'TOTAL' 
FROM giscedata_bt_element;

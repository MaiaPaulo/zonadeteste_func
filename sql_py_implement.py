import asyncio
import asyncpg
import pandas as pd
import geopandas as gpd


async def main():
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    numerodurh = input("coloca ai:")
    data = await conn.fetchrow(f"""
SELECT *
FROM 
   (SELECT max(d.dataenviodurh) AS dataenvio, max(d.numerodurh) AS durh
     FROM durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numerodurh}' and (d.situacaodurh = 'Validada' OR d.situacaodurh = 'Sujeita a outorga')
    ) AS dunica,  
   

	(SELECT 
     sub.fid, sub.cobacia, sub.cocursodag, sub.dn, sub.q_q95espjan, sub.q_q95espfev,
     sub.q_q95espmar, sub.q_q95espabr, sub.q_q95espmai, sub.q_q95espjun, sub.
     ST_Distance(sub.geom, ST_Transform (d.geometry, 3857)) As act_dist
     FROM subtrechos1 AS sub, otto_minibacias_pol_100k AS mini, durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numerodurh}' AND
	       mini.cobacia = (SELECT mini.cobacia
                           FROM
                           durhs_filtradas_completas AS d,
                           otto_minibacias_pol_100k AS mini
                           WHERE
                      		d.numerodurh = '{numerodurh}'
                           AND ST_INTERSECTS(ST_Transform (d.geometry, 3857), mini.wkb_geometry)
                           GROUP BY mini.cobacia
	                      )		 
           AND ST_INTERSECTS(sub.geom, mini.wkb_geometry)
     ORDER BY act_dist
     LIMIT 1
	) As sel 
        """)
    await conn.close()
    print(data)
    return data

data = asyncio.get_event_loop().run_until_complete(main())



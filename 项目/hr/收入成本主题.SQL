 SELECT to_char(a.checkdate, 'yyyymm') months,
             s.buid,
             a.payshopid,
             p.payshopname,
             a.shopid,
             s2.shopname,
             a.docshopid,
             s.shopname,
             a.venderid,
             v.venderlname,
             cg.categoryid,
             cg.categoryname,
             a.chargecodeid,
             c.chargename,
             CASE
               WHEN a.billsource = 0 THEN
                '固定'
               ELSE
                '临时'
             END chargetype,
             SUM(a.chargeamt) chargeamt,
             a.taxrate
        FROM dbusrsettle.view_chargesum@smp68 a
        LEFT JOIN dbusrsettle.category@smp68 cg
          ON (a.majorid = cg.hqcategoryid), dbusrsettle.chargecode@smp68 c,
       dbusrsettle.shop@smp68 s, dbusrsettle.shop@smp68 s2,
       dbusrsettle.vender@smp68 v, dbusrsettle.payshop@smp68 p
       WHERE a.chargecodeid = c.chargecodeid
         AND a.docshopid = s.shopid
         AND a.shopid = s2.shopid
         AND p.buid = v.buid
         AND a.venderid = v.venderid
         AND a.payshopid = p.payshopid
         AND a.checkdate >= d_begin_date
         AND a.checkdate < d_end_date
         AND c.purchaseflag = 1
       GROUP BY to_char(a.checkdate, 'yyyymm'),
                s.buid,
                a.payshopid,
                p.payshopname,
                a.shopid,
                s2.shopname,
                a.docshopid,
                s.shopname,
                a.venderid,
                v.venderlname,
                cg.categoryid,
                cg.categoryname,
                a.chargecodeid,
                c.chargename,
                CASE
                  WHEN a.billsource = 0 THEN
                   '固定'
                  ELSE
                   '临时'
                END,
                a.taxrate;

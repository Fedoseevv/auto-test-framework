WITH CTE AS (
    SELECT FSC.ClientId,
           Cl.ClientName,
           Cl.Type,
           Cl.Form,
           Cl.RegisterDate,
           (NVL(PaymentAmt, 0) + NVL(EnrollementAmt, 0)) as TotalAmt,
           CutoffDt
    FROM FirstShowCase FSC
             JOIN client Cl ON FSC.ClientId=Cl.ClientId
             JOIN Account A ON FSC.AccountId=A.AccountId
    ORDER BY ClientId,
             CutoffDt
)
SELECT ClientId,
       ClientName,
       Type,
       Form,
       RegisterDate,
       SUM(TotalAmt) as TotalAmt,
       CutoffDt
FROM CTE
GROUP BY ClientId,
         ClientName,
         Type,
         Form,
         RegisterDate,
         CutoffDt
ORDER BY ClientId,
         CutoffDt
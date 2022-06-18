SELECT FSC.AccountId,
       A.AccountNum,
       A.DateOpen,
       FSC.ClientId,
       Cl.ClientName,
       (NVL(PaymentAmt, 0) + NVL(EnrollementAmt, 0)) as TotalAmt,
       CutoffDt
FROM FirstShowCase FSC
         JOIN client Cl ON FSC.ClientId=Cl.ClientId
         JOIN Account A ON FSC.AccountId=A.AccountId
ORDER BY AccountId,
         CutoffDt
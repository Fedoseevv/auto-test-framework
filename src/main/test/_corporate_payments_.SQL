WITH WithCourses AS
         (
             SELECT * FROM
                 (
                     SELECT AccountDB,
                            AccountCR,
                            O.Currency,
                            DateOp,
                            Amount,
                            R.Rate,
                            Comment,
                            RateDate,
                            RANK() OVER(PARTITION BY AccountDB, DateOp, O.Currency ORDER BY RateDate DESC) as rnk
                     FROM Operation O INNER JOIN Rate R
                                                 ON O.Currency = R.Currency AND O.DateOp >= R.RateDate
                 )
             WHERE rnk = 1
         ),
     -- Подсчитывает суммы платежей в нац валюте, используя CTE WithCourses
     withTotalAmt AS (
         SELECT AccountDB,
                AccountCR,
                DateOp,
                SUM(Amount * Rate) as AmtNatRate,
                Comment
         FROM WithCourses
         GROUP BY AccountDB,
                  AccountCR,
                  DateOp,
                  Comment
     ),
     -- Сумма операций по счету, где счет клиента указан в дебете проводки
     PaymentAmt AS (
         SELECT AccountDB as AccountId,
                DateOp,
                CAST(SUM(AmtNatRate) AS DECIMAL(20, 2)) as PaymentAmt
         FROM withTotalAmt
         GROUP BY AccountDB,
                  DateOp
     ),
     -- Сумма операций по счету, где счет клиента указан в  кредите проводки
     EnrollementAmt AS (
         SELECT
             AccountCR as AccountId,
             DateOp,
             CAST(SUM(AmtNatRate) AS DECIMAL(20,2)) as EnrollementAmt
         FROM withTotalAmt
         GROUP BY AccountCR,
                  DateOp
     ),
     -- Сумму операций, где счет клиента указан в дебете, и счет кредита 40702
     TaxAmt as (
         SELECT AccountDB as AccountId,
                DateOp,
                CAST(SUM(AmtNatRate) AS DECIMAL(20,2)) as TaxAmt
         FROM (
                  SELECT WTA.AccountDB,
                         WTA.DateOp,
                         WTA.AmtNatRate
                  FROM withTotalAmt WTA JOIN Account a
                                             ON WTA.AccountCR = A.AccountId
                  WHERE A.AccountNum LIKE '40702%'
              )
         GROUP BY AccountDB,
                  DateOp
     ),
     -- Сумма операций, где счет клиента указан в кредите, и счет дебета 40802
     ClearAmt AS (
         SELECT AccountCR as AccountId,
                DateOp,
                CAST(SUM(AmtNatRate) AS DECIMAL(20, 2)) as ClearAmt
         FROM (
                  SELECT WTA.AccountCr,
                         WTA.DateOp,
                         WTA.AmtNatRate
                  FROM withTotalAmt WTA JOIN Account A
                                             ON WTA.AccountDB = A.AccountId
                  WHERE A.AccountNum LIKE '40802%'
              )
         GROUP BY AccountCR,
                  DateOp
     ),
     -- Сумма операций, где счет клиента указан в дебете проводки и назначение платежа не содержит слов по маскам Списка 1
     CarsAmt AS (
         SELECT AccountDB AS AccountId,
                DateOp,
                CAST(SUM(AmtNatRate) AS DECIMAL(20, 2)) as CarsAmt
         FROM (
                  SELECT AccountDB,
                         DateOp,
                         AmtNatRate
                  FROM  withTotalAmt WTA
                  WHERE NOT EXISTS (SELECT Id_exp FROM calculation_params_tech CPT
                                    WHERE CPT.Id_exp = 1 AND WTA.Comment LIKE CPT.Exp_cont)
              )
         GROUP BY AccountDB,
                  DateOp
     ),
     -- Сумма операций, где счет клиента указан в кредите проводки и назначение платежа содержит слова по Маскам Списка 2
     FoodAmt AS (
         SELECT AccountCR AS AccountId,
                DateOp,
                CAST(SUM(AmtNatRate) AS DECIMAL(16, 2)) as FoodAmt
         FROM (
                  SELECT AccountCR,
                         DateOp,
                         AmtNatRate
                  FROM withTotalAmt WTA
                  WHERE EXISTS (SELECT Id_exp FROM calculation_params_tech CPT
                                WHERE CPT.Id_exp = 2 AND WTA.Comment LIKE CPT.Exp_cont)
              )
         GROUP BY AccountCR,
                  DateOp
     ),
     -- Сумма операций с физ. лицами. Счет клиента указан в дебете проводки, а клиент в кредите проводки – ФЛ.
     FlAmt AS (
         SELECT AccountDB as AccountId,
                DateOp,
                CAST(SUM(AmtNatRate) AS DECIMAL(20, 2)) as FlAmt
         FROM (
                  SELECT WTA.AccountDB,
                         WTA.DateOp,
                         WTA.AmtNatRate,
                         A.AccountNum

                  FROM
                      withTotalAmt WTA
                          JOIN Account A ON WTA.AccountCR = A.AccountId
                          JOIN Client Cl ON A.ClientId = Cl.ClientId
                  WHERE Cl.Type = 'Ф'
              )
         GROUP BY AccountDB,
                  DateOp
     ),
     Sample AS (
         SELECT DISTINCT DateOp,
                         AccountId,
                         ClientId
         FROM ACCOUNT CROSS JOIN OPERATION
     )

SELECT
    S.AccountId,
    S.ClientId,
    PA.PaymentAmt,
    EA.EnrollementAmt,
    TA.TaxAmt,
    Cla.ClearAmt,
    ca.CarsAmt,
    fa.FoodAmt,
    FLA.FlAmt,
    S.DateOp as CutoffDt

FROM Sample S
         LEFT JOIN PaymentAmt PA ON S.AccountId=PA.AccountId AND S.DateOp=PA.DateOp
         LEFT JOIN EnrollementAmt EA ON S.AccountId = EA.AccountId AND S.DateOp=EA.DateOp
         LEFT JOIN TaxAmt TA ON TA.AccountId = S.AccountId AND S.DateOp=TA.DateOp
         LEFT JOIN ClearAmt Cla ON Cla.AccountId = S.AccountId AND S.DateOp=Cla.DateOp
         LEFT JOIN CarsAmt ca ON ca.AccountId=S.AccountId AND S.DateOp=ca.DateOp
         LEFT JOIN FoodAmt fa ON fa.AccountId=S.AccountId AND S.DateOp=fa.DateOp
         LEFT JOIN FLAmt FlA ON FLA.AccountId = S.AccountId AND S.DateOp=Fla.DateOp
ORDER BY AccountId,
         CutoffDt

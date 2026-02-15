------------------------------------------------------------
-- SOLUCIONA A LOS CASOS EXP3_S6 - Grupo 9
-- VALERIA SIFONTES Y BASTIAN VALDIVIA
-- procedimientos almacenados para detectar deudores
-- y aplicar multas por no pago de gastos comunes
------------------------------------------------------------

SET SERVEROUTPUT ON;

------------------------------------------------------------
-- PROCEDIMIENTO 1
-- inserta registros en la tabla GASTO_COMUN_PAGO_CERO
-- maneja excepción de clave duplicada
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_insertar_pago_cero(
    p_anno_mes NUMBER,
    p_id_edif NUMBER,
    p_nro_depto NUMBER,
    p_run_adm VARCHAR2,
    p_nombre_adm VARCHAR2,
    p_run_resp VARCHAR2,
    p_nombre_resp VARCHAR2,
    p_nombre_edif VARCHAR2,
    p_multa NUMBER,
    p_obs VARCHAR2
)
IS
BEGIN
    -- inserta el registro en la tabla de deudores
    INSERT INTO GASTO_COMUN_PAGO_CERO
    VALUES (
        p_anno_mes,
        p_id_edif,
        p_nombre_edif,
        p_run_adm,
        p_nombre_adm,
        p_nro_depto,
        p_run_resp,
        p_nombre_resp,
        p_multa,
        p_obs
    );

EXCEPTION
    -- si el registro ya existe, no detiene el proceso
    WHEN DUP_VAL_ON_INDEX THEN
        DBMS_OUTPUT.PUT_LINE(
            'Registro duplicado omitido: Edif '
            || p_id_edif || ' Depto ' || p_nro_depto
        );
END;
/
------------------------------------------------------------
-- PROCEDIMIENTO 2 (PRINCIPAL)
-- detecta departamentos sin pago
-- aplica multas
-- devuelve cantidad de deudores procesados (OUT)
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_proceso_pago_cero(
    p_periodo_actual NUMBER,     -- parámetro IN
    p_valor_uf NUMBER,           -- parámetro IN
    p_total_deudores OUT NUMBER  -- parámetro OUT
)
IS
    --------------------------------------------------------
    -- variables de control de períodos
    --------------------------------------------------------
    v_periodo_anterior NUMBER;
    v_periodo_anterior2 NUMBER;

    --------------------------------------------------------
    -- variables de proceso
    --------------------------------------------------------
    v_multa NUMBER;
    v_obs VARCHAR2(150);
    v_count1 NUMBER;
    v_count2 NUMBER;

    --------------------------------------------------------
    -- cursor que obtiene departamentos del período
    --------------------------------------------------------
    CURSOR c_deptos IS
        SELECT g.id_edif,
               g.nro_depto,
               e.nombre_edif,

               -- RUT administrador
               TO_CHAR(a.numrun_adm, 'FM99G999G999') || '-' || a.dvrun_adm AS run_adm,

               -- nombre administrador 
               INITCAP(a.pnombre_adm || ' ' ||
                       NVL(a.snombre_adm,'') || ' ' ||
                       a.appaterno_adm || ' ' ||
                       NVL(a.apmaterno_adm,'')) AS nombre_adm,

               -- RUT responsable
               TO_CHAR(r.numrun_rpgc, 'FM99G999G999') || '-' || r.dvrun_rpgc AS run_resp,

               -- nombre responsable
               INITCAP(r.pnombre_rpgc || ' ' ||
                       NVL(r.snombre_rpgc,'') || ' ' ||
                       r.appaterno_rpgc || ' ' ||
                       NVL(r.apmaterno_rpgc,'')) AS nombre_resp

        FROM gasto_comun g
        JOIN edificio e ON g.id_edif = e.id_edif
        JOIN administrador a ON e.numrun_adm = a.numrun_adm
        JOIN responsable_pago_gasto_comun r
             ON g.numrun_rpgc = r.numrun_rpgc
        WHERE g.anno_mes_pcgc = p_periodo_actual;

BEGIN
    --------------------------------------------------------
    -- inicializar contador de salida
    --------------------------------------------------------
    p_total_deudores := 0;

    --------------------------------------------------------
    -- calcular períodos anteriores
    --------------------------------------------------------
    v_periodo_anterior  := p_periodo_actual - 1;
    v_periodo_anterior2 := v_periodo_anterior - 1;

    DBMS_OUTPUT.PUT_LINE('Procesando período: ' || p_periodo_actual);

    --------------------------------------------------------
    -- recorrido de departamentos
    --------------------------------------------------------
    FOR rec IN c_deptos LOOP

        -- verificar pago período anterior
        SELECT COUNT(*)
        INTO v_count1
        FROM pago_gasto_comun
        WHERE anno_mes_pcgc = v_periodo_anterior
          AND id_edif = rec.id_edif
          AND nro_depto = rec.nro_depto;

        -- verificar pago dos períodos atrás
        SELECT COUNT(*)
        INTO v_count2
        FROM pago_gasto_comun
        WHERE anno_mes_pcgc = v_periodo_anterior2
          AND id_edif = rec.id_edif
          AND nro_depto = rec.nro_depto;

        -- lógica de multas según reglas de negocio
        IF v_count1 = 0 THEN

            IF v_count2 = 0 THEN
                v_multa := 4 * p_valor_uf;
                v_obs :=
                'Se realizará el corte del combustible y agua a contar del '
                || TO_CHAR(SYSDATE + 5, 'DD/MM/YYYY');
            ELSE
                v_multa := 2 * p_valor_uf;
                v_obs :=
                'Se realizará el corte del combustible y agua';
            END IF;

            -- insertar en tabla de deudores
            sp_insertar_pago_cero(
                p_periodo_actual,
                rec.id_edif,
                rec.nro_depto,
                rec.run_adm,
                rec.nombre_adm,
                rec.run_resp,
                rec.nombre_resp,
                rec.nombre_edif,
                v_multa,
                v_obs
            );

            -- actualizar multa en gasto común
            UPDATE gasto_comun
            SET multa_gc = v_multa
            WHERE anno_mes_pcgc = p_periodo_actual
              AND id_edif = rec.id_edif
              AND nro_depto = rec.nro_depto;

            -- aumentar contador OUT
            p_total_deudores := p_total_deudores + 1;

        END IF;

    END LOOP;

    COMMIT;

    DBMS_OUTPUT.PUT_LINE(
        'Total de departamentos con pago cero: '
        || p_total_deudores
    );

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error en el proceso: ' || SQLERRM);
        ROLLBACK;
END;
/
------------------------------------------------------------
-- EJECUCIÓN CON VARIABLES BIND
------------------------------------------------------------

-- variable bind de salida
VARIABLE v_total NUMBER;

BEGIN
    sp_proceso_pago_cero(
        TO_NUMBER(TO_CHAR(SYSDATE,'YYYY')||'05'),
        29509,
        :v_total
    );
END;
/

PRINT v_total;

------------------------------------------------------------
-- CONSULTAS DE VERIFICACIÓN
------------------------------------------------------------

SELECT *
FROM GASTO_COMUN_PAGO_CERO
ORDER BY nombre_edif, nro_depto;

SELECT anno_mes_pcgc,
       id_edif,
       nro_depto,
       fecha_desde_gc,
       fecha_hasta_gc,
       multa_gc
FROM GASTO_COMUN
WHERE multa_gc > 0
ORDER BY id_edif, nro_depto;

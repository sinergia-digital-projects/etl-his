<?php

namespace App\Service;

use Doctrine\DBAL\Connection;

/**
 * Constructor de esquema para la base de datos de análisis en PostgreSQL.
 *
 * Responsabilidad:
 *  - (Re)crear por completo el esquema de trabajo (schema public) de forma transaccional,
 *    eliminando todo lo existente y generando tablas e índices necesarios para el ETL.
 *
 * Advertencia:
 *  - El método recreate() hace DROP SCHEMA ... CASCADE. Esto borra TODO el contenido previo.
 *    Úsalo solo en entornos controlados (p.ej., staging/analítica), no en producción con datos valiosos.
 */
class PostgresSchemaBuilder
{
	/**
	 * Conexión a PostgreSQL vía Doctrine DBAL.
	 */
	private Connection $conn;

	/**
	 * @param Connection $conn Conexión activa a la BD de destino (PostgreSQL).
	 */
	public function __construct(Connection $conn)
	{
		$this->conn = $conn;
	}

	/**
	 * (Re)crea el esquema completo de la BD de destino.
	 *
	 * Pasos:
	 *  1) Inicia transacción.
	 *  2) Elimina y vuelve a crear el schema public, otorgando permisos básicos.
	 *  3) Crea tablas: paciente, turno, prestacion, prestacion_x_turno.
	 *  4) Genera índices para acelerar consultas frecuentes.
	 *  5) Confirma transacción; ante error revierte todo.
	 *
	 * @throws \Throwable Si ocurre cualquier error durante la reconstrucción.
	 */
	public function recreate(): void
	{
		$this->conn->beginTransaction();

		try {
			// 1) Reset de esquema: elimina el schema "public" y todo su contenido.
			$this->conn->executeStatement('DROP SCHEMA IF EXISTS public CASCADE');

			// 2) Vuelve a crearlo y otorga permisos básicos.
			$this->conn->executeStatement('CREATE SCHEMA public');
			$this->conn->executeStatement('GRANT ALL ON SCHEMA public TO PUBLIC');

			// 3) Tablas base del modelo analítico:

			// Tabla de pacientes
			$this->conn->executeStatement(<<<'SQL'
CREATE TABLE paciente (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    apellido VARCHAR(255) NOT NULL,
    documento_identidad VARCHAR(255) NOT NULL,
    sexo_inferido VARCHAR(255)
);
SQL);
			// Índice para acelerar búsquedas por documento
			$this->conn->executeStatement('CREATE INDEX idx_paciente_doc ON paciente (documento_identidad)');

			// Tabla de turnos (FK a paciente)
			$this->conn->executeStatement(<<<'SQL'
CREATE TABLE turno (
    id SERIAL PRIMARY KEY,
    paciente_id INTEGER NOT NULL,
    fecha DATE NOT NULL,
    hora TIME(0) WITHOUT TIME ZONE NOT NULL,
    duracion_minutos INTEGER NOT NULL,
    sobreturno BOOLEAN NOT NULL,
    estado VARCHAR(255) NOT NULL,
    fecha_alta TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    usuario_alta VARCHAR(255) NOT NULL,
    CONSTRAINT fk_turno_paciente FOREIGN KEY (paciente_id)
        REFERENCES paciente (id) ON DELETE RESTRICT
);
SQL);
			// Índices típicos de consulta por relaciones y filtros comunes
			$this->conn->executeStatement('CREATE INDEX idx_turno_paciente ON turno (paciente_id)');
			$this->conn->executeStatement('CREATE INDEX idx_turno_fecha ON turno (fecha)');
			$this->conn->executeStatement('CREATE INDEX idx_turno_estado ON turno (estado)');

			// Catálogo de prestaciones (únicas por nombre)
			$this->conn->executeStatement(<<<'SQL'
CREATE TABLE prestacion (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL UNIQUE
);
SQL);

			// Tabla pivote N:N entre turno y prestacion
			$this->conn->executeStatement(<<<'SQL'
CREATE TABLE prestacion_x_turno (
    id SERIAL PRIMARY KEY,
    turno_id INTEGER NOT NULL,
    prestacion_id INTEGER NOT NULL,
    CONSTRAINT fk_pxt_turno FOREIGN KEY (turno_id) REFERENCES turno (id) ON DELETE CASCADE,
    CONSTRAINT fk_pxt_prestacion FOREIGN KEY (prestacion_id) REFERENCES prestacion (id) ON DELETE RESTRICT
);
SQL);
			// Índices para joins eficientes en la tabla pivote
			$this->conn->executeStatement('CREATE INDEX idx_pxt_turno ON prestacion_x_turno (turno_id)');
			$this->conn->executeStatement('CREATE INDEX idx_pxt_prestacion ON prestacion_x_turno (prestacion_id)');

			// 4) Confirmación de la reconstrucción del esquema
			$this->conn->commit();
		} catch (\Throwable $e) {
			// Ante cualquier fallo, revierte todos los cambios del batch.
			$this->conn->rollBack();
			throw $e;
		}
	}
}

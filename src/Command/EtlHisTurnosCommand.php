<?php

namespace App\Command;

use App\Service\PostgresSchemaBuilder;
use Doctrine\DBAL\Connection;
use GenderDetector\GenderDetector;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(
	name: 'app:etl-his-turnos',
	description: 'Ejecuta el proceso de ETL para los turnos del HIS y los inserta en una base de datos postgres de analisis.',
)]
class EtlHisTurnosCommand extends Command
{
	/**
	 * Conexión a la base de datos destino (PostgreSQL) usando Doctrine DBAL.
	 */
	private Connection $connection;

	/**
	 * Servicio responsable de (re)crear el esquema de la base de datos destino.
	 */
	private PostgresSchemaBuilder $postgresSchemaBuilder;

	/**
	 * Parámetros de conexión a SQL Server (fuente).
	 */
	private string $sqlsrvHost;
	private string $sqlsrvDb;
	private string $sqlsrvUser;
	private string $sqlsrvPass;

	/**
	 * Inyecta dependencias y credenciales de SQL Server.
	 *
	 * @param Connection            $connection            Conexión a la BD destino (PostgreSQL)
	 * @param PostgresSchemaBuilder $postgresSchemaBuilder Servicio para recrear el esquema
	 * @param string                $sqlsrvHost            Host de SQL Server
	 * @param string                $sqlsrvDb              Base de datos de SQL Server
	 * @param string                $sqlsrvUser            Usuario de SQL Server
	 * @param string                $sqlsrvPass            Password de SQL Server
	 */
	public function __construct(
		Connection $connection,
		PostgresSchemaBuilder $postgresSchemaBuilder,
		string $sqlsrvHost,
		string $sqlsrvDb,
		string $sqlsrvUser,
		string $sqlsrvPass,
	) {
		$this->connection = $connection;
		$this->postgresSchemaBuilder = $postgresSchemaBuilder;
		$this->sqlsrvHost = $sqlsrvHost;
		$this->sqlsrvDb = $sqlsrvDb;
		$this->sqlsrvUser = $sqlsrvUser;
		$this->sqlsrvPass = $sqlsrvPass;
		parent::__construct();
	}

	/**
	 * Punto de entrada del comando.
	 *
	 * Flujo general:
	 *  1) Extrae datos desde SQL Server (HIS).
	 *  2) Inicia transacción en PostgreSQL.
	 *  3) Recrea el esquema destino (DROP/CREATE de tablas según implementación del builder).
	 *  4) Transforma y carga datos: Pacientes, Turnos y Prestaciones (+ tabla pivote).
	 *  5) Confirma transacción o revierte si hay errores.
	 */
	protected function execute(InputInterface $input, OutputInterface $output): int
	{
		$io = new SymfonyStyle($input, $output);
		$io->title('Inicio del Proceso de ETL de Turnos del HIS');

		try {
			// 1) EXTRACCIÓN
			$io->section('1. Extracción de Datos');
			$io->text('Obteniendo datos desde SQL Server...');
			$data = $this->getSQLServerData();

			// Si no hay datos o falló la extracción, se informa y termina sin error (SUCCESS).
			if (Command::FAILURE === $data || empty($data)) {
				$io->warning('No se encontraron datos para procesar o hubo un error en la extracción.');

				return Command::SUCCESS;
			}

			$io->success(count($data).' registros extraídos.');

			// 2) TRANSFORMACIÓN + CARGA
			$io->section('2. Transformación y Carga de Datos');

			if (!$io->confirm('ADVERTENCIA: Este proceso eliminará TODOS los datos existentes en la base de datos de análisis y recreará su estructura. ¿Desea continuar?', false)) {
				$io->warning('Operación cancelada por el usuario.');

				return Command::SUCCESS;
			}

			$this->connection->beginTransaction();
			$io->text('Transacción iniciada en la base de datos de destino.');

			// Recrea completamente el esquema destino para asegurar consistencia del dataset.
			$io->text('Recreando esquema de la base de datos destino.');
			$this->postgresSchemaBuilder->recreate();
			$io->text('Creación del esquema de la base de datos destino finalizada.');

			// Caches en memoria para evitar consultas repetitivas
			$pacientesCache = [];     // [documento => paciente_id]
			$prestacionesCache = [];  // [nombrePrestacion => prestacion_id]

			$contador = 0;
			$total = count($data);
			$io->progressStart($total);

			foreach ($data as $row) {
				// Normaliza y busca/crea paciente
				$documento = trim($row['paciente_documento']);
				$pacienteId = null;

				if (isset($pacientesCache[$documento])) {
					// Si ya lo tenemos en cache, usamos el id cacheado (evita re-consultas)
					$pacienteId = $pacientesCache[$documento];
				} else {
					// Busca paciente existente en la BD destino por documento
					$pacienteId = $this->connection->fetchOne('SELECT id FROM paciente WHERE documento_identidad = ?', [$documento]);

					if (!$pacienteId) {
						// Si no existe, inserta paciente con nombre y apellido limpios, e intenta inferir sexo
						$nombreLimpio = $this->limpiarNombres($output, $row['paciente_nombre']);
						$apellidoLimpio = $this->limpiarNombres($output, $row['paciente_apellido']);
						// Toma el primer nombre para inferencia de sexo
						$sexoInferido = $this->inferirSexo($output, explode(' ', $nombreLimpio)[0]);

						$sql = 'INSERT INTO paciente (nombre, apellido, documento_identidad, sexo_inferido) VALUES (?, ?, ?, ?) RETURNING id';

						$pacienteId = $this->connection->fetchOne($sql, [
							$nombreLimpio,
							$apellidoLimpio,
							$documento,
							$sexoInferido,
						]);
					}

					// Cachea el id de paciente por documento
					$pacientesCache[$documento] = $pacienteId;
				}

				// Inserta el turno asociado al paciente
				$sql = 'INSERT INTO turno (paciente_id, fecha, hora, duracion_minutos, sobreturno, estado, fecha_alta, usuario_alta) VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id';

				$turnoId = $this->connection->fetchOne($sql, [
					$pacienteId,
					$row['turno_fecha'],
					$row['turno_hora'],
					$row['turno_duracion'],
					$row['sobreturno'],
					$row['turno_estado'],
					$row['turnos_fecha_alta'],
					$row['usuario_alta_usuario'],
				]);

				if (!$turnoId) {
					// Si la BD no retorna id, considera que algo falló en la inserción
					throw new \RuntimeException('No se pudo obtener el ID del turno insertado.');
				}

				// Inserta prestaciones vinculadas al turno (hasta 11 columnas: prestacion0..prestacion10)
				for ($i = 0; $i <= 10; ++$i) {
					$columnaPrestacion = 'prestacion'.$i;
					if (!empty($row[$columnaPrestacion])) {
						$nombrePrestacion = trim($row[$columnaPrestacion]);
						$prestacionId = null;

						if (isset($prestacionesCache[$nombrePrestacion])) {
							// Usa cache de prestación si ya fue insertada/buscada
							$prestacionId = $prestacionesCache[$nombrePrestacion];
						} else {
							// Busca prestación por nombre
							$prestacionId = $this->connection->fetchOne('SELECT id FROM prestacion WHERE nombre = ?', [$nombrePrestacion]);

							// Si no existe, la crea
							if (!$prestacionId) {
								$sql = 'INSERT INTO prestacion (nombre) VALUES (?) RETURNING id';
								$prestacionId = $this->connection->fetchOne($sql, [$nombrePrestacion]);
							}
							// Cachea id de prestación por nombre
							$prestacionesCache[$nombrePrestacion] = $prestacionId;
						}

						// Inserta relación N-N entre Turno y Prestación
						$this->connection->insert('prestacion_x_turno', [
							'turno_id' => $turnoId,
							'prestacion_id' => $prestacionId,
						]);
					}
				}

				$io->progressAdvance();
				++$contador;
			}

			// Si todo salió bien, cierra transacción
			$this->connection->commit();
			$io->progressFinish();
			$io->success("Proceso completado. Se han procesado e insertado $contador registros.");
		} catch (\Throwable $e) {
			// Cualquier error revierte la transacción y muestra detalles
			$this->connection->rollBack();
			if ($this->connection->isTransactionActive()) {
				$io->error('Se ha producido un error y la transacción ha sido revertida.');
			}
			$io->error($e->getMessage());
			$io->text($e->getTraceAsString());

			return Command::FAILURE;
		}

		return Command::SUCCESS;
	}

	/**
	 * Obtiene los datos desde SQL Server usando PDO.
	 *
	 * Importante:
	 *  - Usa cifrado y confía en el certificado del servidor (TrustServerCertificate=YES).
	 *  - Retorna un array de filas asociativas o Command::FAILURE si ocurre un error.
	 *
	 * @return array<int, array<string, mixed>>|int
	 */
	private function getSQLServerData()
	{
		try {
			// DSN con cifrado habilitado
			$dsn = sprintf(
				'sqlsrv:Server=%s;Database=%s;Encrypt=YES;TrustServerCertificate=YES',
				$this->sqlsrvHost,
				$this->sqlsrvDb
			);

			// Conexión PDO con manejo de errores por excepciones
			$pdo = new \PDO(
				$dsn,
				$this->sqlsrvUser,
				$this->sqlsrvPass,
				[\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION]
			);

			// Consulta base que trae Turnos con datos enriquecidos (paciente, usuario, prestaciones, etc.)
			$sql = "
select
    t.Id as turno_id,
    ttprevisto.Nombre as turno_tipo,
    rt.Nombre as recurso_tipo,
    s.Nombre as servicio,
    ca.Nombre as centro_atencion,

    -- paciente
    p.Nombres as paciente_nombre,
    p.Apellido as paciente_apellido,
    p.Documento_Numero as paciente_documento,

    -- turno
    t.FechaTurno as turno_fecha,
    t.HoraTurno as turno_hora,
    t.DuracionMinutos as turno_duracion,
    t.EsSobreTurno as sobreturno,
    te.Nombre as turno_estado,

    -- auditoria
    t.FechaAlta as turnos_fecha_alta,
    t.IdUsuario_Otorgo as usuario_alta_id,
    usu.NombreInicioSesion as usuario_alta_usuario,
    per.Nombres + ' ' + per.Apellido as usuario_alta_nombre,

    -- prestaciones
    pres0.Nombre as prestacion0,
    pres1.Nombre as prestacion1,
    pres2.Nombre as prestacion2,
    pres3.Nombre as prestacion3,
    pres4.Nombre as prestacion4,
    pres5.Nombre as prestacion5,
    pres6.Nombre as prestacion6,
    pres7.Nombre as prestacion7,
    pres8.Nombre as prestacion8,
    pres9.Nombre as prestacion9,
    pres10.Nombre as prestacion10

from turnos t
         JOIN Recursos r on r.Id = t.IdRecurso
         JOIN Recurso_Tipos rt on rt.Id = r.IdRecurso_Tipo
         JOIN Servicios s on s.Id = t.IdServicio
         JOIN CentrosAtencion ca on ca.Id = t.IdCentroAtencion
         JOIN Personas p on p.Id = t.IdPersona
         JOIN Turno_Estados te on te.Id = t.IdTurno_Estado
         JOIN Usuarios usu on usu.id = t.IdUsuario_Otorgo
         JOIN Personas per on per.Id = usu.IdPersona
         -- informes
         JOIN Turno_Tipos ttprevisto on ttprevisto.Id = t.IdTurno_TipoPrevisto
         LEFT JOIN RIS.OrdenDeTrabajo ot on ot.IdTurno = t.Id
         LEFT JOIN RIS.Informes inf on inf.IdOrdenDeTrabajo = ot.Id
         LEFT JOIN Turno_Estados te_ot on te_ot.Id = ot.IdEstado
         LEFT JOIN Turno_Estados te_inf on te_inf.Id = inf.IdEstadoActual
         -- prestaciones
         LEFT JOIN Prestaciones pres0 on pres0.Id = t.IdPrestacionAsignada
         LEFT JOIN Prestaciones pres1 on pres1.Id = t.IdPrestacionRealizable01
         LEFT JOIN Prestaciones pres2 on pres2.Id = t.IdPrestacionRealizable02
         LEFT JOIN Prestaciones pres3 on pres3.Id = t.IdPrestacionRealizable03
         LEFT JOIN Prestaciones pres4 on pres4.Id = t.IdPrestacionRealizable04
         LEFT JOIN Prestaciones pres5 on pres5.Id = t.IdPrestacionRealizable05
         LEFT JOIN Prestaciones pres6 on pres6.Id = t.IdPrestacionRealizable06
         LEFT JOIN Prestaciones pres7 on pres7.Id = t.IdPrestacionRealizable07
         LEFT JOIN Prestaciones pres8 on pres8.Id = t.IdPrestacionRealizable08
         LEFT JOIN Prestaciones pres9 on pres9.Id = t.IdPrestacionRealizable09
         LEFT JOIN Prestaciones pres10 on pres10.Id = t.IdPrestacionRealizable10

ORDER BY t.FechaAlta DESC
            ";
			$stmt = $pdo->query($sql);
			$result = $stmt->fetchAll();

			return $result;
		} catch (\Throwable $e) {
			// Si hay error en la extracción, se devuelve FAILURE y el flujo superior lo maneja.
			return Command::FAILURE;
		}
	}

	/**
	 * Limpia y normaliza nombres/apellidos:
	 *  - Recorta espacios extremos
	 *  - Colapsa múltiples espacios intermedios
	 *  - Convierte a MAYÚSCULAS (UTF-8)
	 */
	private function limpiarNombres(OutputInterface $output, ?string $txt): ?string
	{
		if (null === $txt) {
			return null;
		}

		$ret = trim($txt);
		$ret = preg_replace('/\s+/', ' ', $ret);

		return mb_strtoupper($ret, 'UTF-8');
	}

	/**
	 * Intenta inferir el sexo a partir del primer nombre usando la librería GenderDetector.
	 * En caso de error, se informa por salida de consola y retorna null.
	 *
	 * @param string|null $nombre Primer nombre (p.ej., "Juan")
	 *
	 * @return string|null Valor inferido (p.ej., "male", "female") o null si no se puede determinar
	 */
	private function inferirSexo(OutputInterface $output, ?string $nombre): ?string
	{
		try {
			$detector = new GenderDetector();

			// 'PY' indica el conjunto de datos/reglas para Paraguay (ajustar si se requiere otro país)
			$ret = $detector->getGender($nombre, 'PY');

			return $ret?->name;
		} catch (\Exception $e) {
			$message = "Error en librería de inferencia de sexo para '$nombre': ".$e->getMessage();

			$output->writeln('<error>'.$message.'</error>');

			return null;
		}
	}
}

package lsi.ubu.servicios;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.CompraBilleteTrenException;
import lsi.ubu.util.PoolDeConexiones;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	@Override
	public void anularBillete(Time hora, java.util.Date fecha, String origen, String destino, int nroPlazas, int ticket)
			throws SQLException {

		Connection conn = null;
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		PreparedStatement stDatos = null;
		PreparedStatement stUpdate = null;
		PreparedStatement stDelete = null;
		ResultSet rs = null;

		int rsRows = 0;
		int idViaje = 0;
		int nRowsDeleted = 0;

		java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
		java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());

		try {
			conn = pool.getConnection();

			// Realizamos la consulta con un bloqueo pesimista para evitar problemas de
			// concurrencia
			// En este caso hemos aplicado un FOR UPDATE para asegurar un bloque directo
			// aunque se sacrifique portabilidad
			String query = "SELECT v.idViaje, t.cantidad"
					+ " FROM viajes v "
					+ " JOIN recorridos r ON r.idRecorrido = v.idRecorrido"
					+ " JOIN tickets t ON t.idViaje = v.idViaje "
					+ " WHERE idTicket = ? AND v.fecha = ? AND r.horasalida - TRUNC(r.horaSalida) = ? - TRUNC(?)"
					+ " FOR UPDATE";
			stDatos = conn.prepareStatement(query);
			stDatos.setInt(1, ticket);
			stDatos.setDate(2, fechaSqlDate);
			stDatos.setTimestamp(3, horaTimestamp);
			stDatos.setTimestamp(4, horaTimestamp);

			rs = stDatos.executeQuery();

			// Comprobamos si el billete existe
			if (rs.next()) {
				// Comprobamos si el billete tiene plazas suficientes para anular
				if (nroPlazas <= rs.getInt("cantidad")) {

					idViaje = rs.getInt("idViaje");

					// Actualizamos el número de plazas libres
					String update = "UPDATE viajes SET nPlazasLibres = nPlazasLibres + ? WHERE idViaje = ?";
					stUpdate = conn.prepareStatement(update);
					stUpdate.setInt(1, nroPlazas);
					stUpdate.setInt(2, idViaje);
					stUpdate.executeUpdate();

					// Eliminamos el billete
					String delete = "DELETE FROM tickets WHERE idTicket = ?";
					stDelete = conn.prepareStatement(delete);
					stDelete.setInt(1, ticket);
					nRowsDeleted = stDelete.executeUpdate();

					// Esta excepción no debería de lanzarse nunca ya que se ha comprobado que los
					// requisitos se cumplan.
					// Pero hemos decidido añadirlo por si acaso
					if (nRowsDeleted == 0) {
						throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_ANULADO);
					}

					conn.commit();

				} else {
					throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_EXISTE_TICKET_ANULAR);
				}
			} else {
				throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_EXISTE_TICKET);
			}

		} catch (SQLException e) {
			conn.rollback();
			if (e instanceof CompraBilleteTrenException) {
				throw e;
			} else {
				LOGGER.error(e.getMessage());
			}

		} finally {
			conn.setAutoCommit(true);

			if (conn != null)
				conn.close();

			if (rs != null)
				rs.close();

			if (stDatos != null)
				stDatos.close();

			if (stUpdate != null)
				stUpdate.close();

			if (stDelete != null)
				stDelete.close();
		}
	}

	@Override
	public void comprarBillete(Time hora, Date fecha, String origen, String destino, int nroPlazas)
			throws SQLException {
		Connection conn = null;
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		PreparedStatement stDatos = null;
		PreparedStatement stUpdate = null;
		PreparedStatement stInsert = null;
		ResultSet rs = null;

		int nRowsCreated = 0;
		int nRowsUpdated = 0;
		int idViaje = 0;

		java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
		java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());

		try {
			conn = pool.getConnection();
			conn.setAutoCommit(false);

			// Al igual que en el caso anterior hemos aplicado un bloqueo pesimista para
			// evitar problemas de concurrencia.
			// Hemos aplicadoaplicado un FOR UPDATE para asegurar un bloque directo aunque
			// se sacrifique portabilidad
			// Para el tratamiento de las horas, hemos utilizado trunc para eliminar la
			// parte de la fecha de la columna horaSalida,
			// dejando solo la parte del tiempo, y luego restando horaSalida completa para
			// aislar específicamente la parte del tiempo.
			String query = "SELECT v.idViaje, r.precio"
					+ " FROM recorridos r "
					+ " JOIN viajes v ON r.idRecorrido = v.idRecorrido"
					+ " WHERE r.horasalida - TRUNC(r.horaSalida) = ? - TRUNC(?) AND v.fecha = ? AND r.estacionOrigen = ? AND r.estacionDestino = ?"
					+ " FOR UPDATE";
			int contador = 1;
			stDatos = conn.prepareStatement(query);
			stDatos.setTimestamp(contador++, horaTimestamp);
			stDatos.setTimestamp(contador++, horaTimestamp);
			stDatos.setDate(contador++, fechaSqlDate);
			stDatos.setString(contador++, origen);
			stDatos.setString(contador++, destino);

			rs = stDatos.executeQuery();

			// Comprobamos si existe el viaje
			if (rs.next()) {

				idViaje = rs.getInt("idViaje");
				contador = 1;

				// Actualizamos el número de plazas libres
				String restarPlazas = "UPDATE viajes SET nPlazasLibres = nPlazasLibres - ? WHERE idViaje = ? AND nPlazasLibres >= ?";
				stUpdate = conn.prepareStatement(restarPlazas);
				stUpdate.setInt(contador++, nroPlazas);
				stUpdate.setInt(contador++, idViaje);
				stUpdate.setInt(contador++, nroPlazas);
				nRowsUpdated = stUpdate.executeUpdate();

				// Comprobamos si hay plazas suficientes
				if (nRowsUpdated > 0) {
					contador = 1;
					BigDecimal precio = rs.getBigDecimal("precio");
					BigDecimal precioTotal = precio.multiply(new BigDecimal(nroPlazas));

					// Insertamos el billete
					String insertTicket = "INSERT INTO tickets (idTicket, idViaje, fechaCompra, cantidad, precio) VALUES (seq_tickets.NEXTVAL, ?, TRUNC(current_date), ?, ?)";
					stInsert = conn.prepareStatement(insertTicket);
					stInsert.setInt(contador++, idViaje);
					stInsert.setInt(contador++, nroPlazas);
					stInsert.setBigDecimal(contador, precioTotal);
					nRowsCreated = stInsert.executeUpdate();

					// Esta excepción no debería de lanzarse nunca ya que se ha comprobado que los
					// requisitos se cumplan
					// Pero hemos decidido añadirlo por si acaso
					if (nRowsCreated == 0) {
						throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_COMPRADO);
					}

					conn.commit();
				} else {
					throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_PLAZAS);
				}

			} else {
				throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_EXISTE_VIAJE);
			}

		} catch (SQLException e) {
			conn.rollback();
			if (e instanceof CompraBilleteTrenException) {
				throw e;
			} else {
				LOGGER.error(e.getMessage());
			}

		} finally {
			conn.setAutoCommit(true);

			if (rs != null)
				rs.close();

			if (stDatos != null)
				stDatos.close();

			if (stUpdate != null)
				stUpdate.close();

			if (stInsert != null)
				stInsert.close();

			if (conn != null)
				conn.close();

		}
	}
}
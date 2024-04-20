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
		int nRowsDeleted = 0;
		
		java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
		java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());
		
		ResultSet rs = null;
		int rsRows = 0;
		int idViaje = 0;
		
		try {
			conn = pool.getConnection();
			String query = 
					"SELECT v.idViaje, t.cantidad "
					+ "FROM viajes v "
					+ "JOIN tickets t ON t.idViaje = v.idViaje "
					+ "WHERE idTicket = ?";
			stDatos = conn.prepareStatement(query);
			stDatos.setInt(1, ticket);
			
			rs = stDatos.executeQuery();
			
			if(rs.next()) {
				if(nroPlazas < rs.getInt("cantidad")) {
					idViaje = rs.getInt("idViaje");
					String update = 
							"UPDATE viajes SET nPlazasLibres = nPlazasLibres + ? WHERE idViaje = ?";
					
					stUpdate = conn.prepareStatement(update);
					stUpdate.setInt(1, nroPlazas);
					stUpdate.setInt(1, idViaje);
					
					stUpdate.executeUpdate();
					
					String delete =
							"DELETE FROM tickets WHERE id = ?";
					
					stDelete = conn.prepareStatement(delete);
					stDelete.setInt(1, ticket);
					
					nRowsDeleted = stDelete.executeUpdate();
					
					if(nRowsDeleted == 0) {
						throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_ANULADO);
					}
					
				}
				else {
					throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_EXISTE_TICKET_ANULAR);
				}
			}
			
		}catch(SQLException e){
			conn.rollback();
			if(e instanceof CompraBilleteTrenException) {
				throw e;
			}else {
				LOGGER.error(e.getMessage());
			}
			
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
				conn.close();
			}
			else if(rs == null) {
				conn.setAutoCommit(true);
				rs.close();
			}
			else if(stDatos == null) {
				conn.setAutoCommit(true);
				stDatos.close();
			}
			else if(stUpdate == null) {
				conn.setAutoCommit(true);
				stUpdate.close();
			}
			else if(stDelete == null) {
				conn.setAutoCommit(true);
				stDelete.close();
			}
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
		
		java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
		java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());
		
		ResultSet rs = null;
		int nRowsUpdated = 0;
		int idViaje = 0;
		
		try {
			conn = pool.getConnection();
			conn.setAutoCommit(false);
			
			LOGGER.info("Parámetros - Origen: {}, Destino: {}, Hora: {}, Fecha: {}", origen, destino, horaTimestamp, fechaSqlDate);
			
			int contador = 1;
			String query = 
					"SELECT v.idViaje, r.precio "
					+ "FROM recorridos r "
					+ "JOIN viajes v ON r.idRecorrido = v.idRecorrido "
					+ "WHERE r.horasalida - TRUNC(r.horaSalida) = ? - TRUNC(?) AND v.fecha = ? AND r.estacionOrigen = ? AND r.estacionDestino = ?";
			
			stDatos = conn.prepareStatement(query);
			stDatos.setTimestamp(contador++, horaTimestamp);
			stDatos.setTimestamp(contador++, horaTimestamp);
			stDatos.setDate(contador++, fechaSqlDate);
			stDatos.setString(contador++, origen); 
			stDatos.setString(contador++, destino);
	        
	        rs = stDatos.executeQuery();
			
	        if(rs.next()) {
	        	idViaje = rs.getInt("idViaje");
	        	
	        	contador = 1;
		        String restarPlazas = 
		        		"UPDATE viajes SET nPlazasLibres = nPlazasLibres - ? WHERE idViaje = ? AND nPlazasLibres >= ?";
		        		
		        stUpdate = conn.prepareStatement(restarPlazas);
		        stUpdate.setInt(contador++, nroPlazas);
		        stUpdate.setInt(contador++, idViaje);
		        stUpdate.setInt(contador++, nroPlazas);
		        
		        nRowsUpdated= stUpdate.executeUpdate();
		        
		        if(nRowsUpdated > 0) {
		        	contador = 1;
		        	BigDecimal precio = rs.getBigDecimal("precio");
		        	BigDecimal precioTotal = precio.multiply(new BigDecimal(nroPlazas));
				    String insertTicket = "INSERT INTO tickets (idTicket, idViaje, fechaCompra, cantidad, precio) VALUES (seq_tickets.NEXTVAL, ?, TRUNC(current_date), ?, ?)";
			
				    stInsert = conn.prepareStatement(insertTicket);
									
					stInsert.setInt(contador++, idViaje);
					stInsert.setInt(contador++, nroPlazas);
					stInsert.setBigDecimal(contador, precioTotal);
					
					stInsert.executeUpdate();
		        }else{
		        	throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_PLAZAS);
		        }
		        
		    }else {
		    	throw new CompraBilleteTrenException(CompraBilleteTrenException.NO_EXISTE_VIAJE);
	        }

		}catch(SQLException e){
			conn.rollback();
			if(e instanceof CompraBilleteTrenException) {
				throw e;
			}else {
				LOGGER.error(e.getMessage());
			}
			
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
				conn.close();
			}
			else if(rs == null) {
				conn.setAutoCommit(true);
				rs.close();
			}
			else if(stDatos == null) {
				conn.setAutoCommit(true);
				stDatos.close();
			}
			else if(stUpdate == null) {
				conn.setAutoCommit(true);
				stUpdate.close();
			}
			else if(stInsert == null) {
				conn.setAutoCommit(true);
				stInsert.close();
			}
		}
	}
}

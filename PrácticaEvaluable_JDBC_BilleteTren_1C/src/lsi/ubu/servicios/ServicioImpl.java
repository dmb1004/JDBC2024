package lsi.ubu.servicios;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.util.PoolDeConexiones;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	@Override
	public void anularBillete(Time hora, java.util.Date fecha, String origen, String destino, int nroPlazas, int ticket)
			throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		/* Conversiones de fechas y horas */
		java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
		java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		// A completar por el alumno
	}

	@Override
	public void comprarBillete(Time hora, Date fecha, String origen, String destino, int nroPlazas)
			throws SQLException {
	    LOGGER.info("Obteniendo ID de viaje para Hora: {}, Origen: {}, Destino: {}", hora, origen, destino);
		Connection conn = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		int contador = 1;
		int idViaje = 0;
		int precio = 0;
		
		try {
			PoolDeConexiones pool = PoolDeConexiones.getInstance();
			conn = pool.getConnection();
			conn.setAutoCommit(false);
			
			/* Conversiones de fechas y horas */
			java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
			java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());
			
			idViaje = obtenerIdViaje(conn, hora, origen, destino);
			
		    String query = "INSERT INTO tickets (idTicket, idViaje, fechaCompra, cantidad, precio) VALUES (seq_tickets.NEXTVAL, ?, ?, ?, ?)";
	
			st = conn.prepareStatement(query);
			
			precio = nroPlazas * obtenerPrecio(conn, hora, origen, destino);
			
			st.setInt(contador++, idViaje);
			st.setDate(contador++, fechaSqlDate);
			st.setInt(contador++, nroPlazas);
			st.setInt(contador, precio);
			
			st.executeUpdate();

		}catch(SQLException e){
			if(conn == null) {
				conn.rollback();
			}
			throw e;
		}
		finally {
			if(conn != null) {
				conn.setAutoCommit(true);
				conn.close();
			}
			
		}
	}
	
	public int obtenerIdViaje(Connection conn, Time hora, String origen, String destino) throws SQLException {
	    LOGGER.info("Obteniendo ID de viaje para Hora: {}, Origen: {}, Destino: {}", hora, origen, destino);
	    String query = "SELECT v.idViaje FROM recorridos r JOIN viajes v ON v.idRecorrido = r.idRecorrido WHERE r.horaSalida = ? AND r.estacionOrigen = ? AND r.estacionDestino = ?";
	    try (PreparedStatement st = conn.prepareStatement(query)) {
	        st.setTime(1, hora); 
	        st.setString(2, origen); 
	        st.setString(3, destino); 
	        try (ResultSet rs = st.executeQuery()) {
	            if (rs.next()) {
	                return rs.getInt("idViaje");
	            } else {
	                throw new SQLException("No existe viaje para tal fecha, origen y destino.");
	            }
	        }
	    } catch (SQLException e) {
	        LOGGER.error("Error obteniendo el ID del viaje", e);
	        throw e;
	    }
	}

	
	public int obtenerPrecio(Connection conn, Time hora, String origen, String destino) throws SQLException{
		PreparedStatement st = null;
		ResultSet rs = null;
		int precio = 0;
		int contador = 1;
			    
		try {
		    String viaje = "SELECT recorridos.precio FROM recorridos WHERE horaSalida = ? AND estacionOrigen = ? AND estacionDestino = ?";
			st = conn.prepareStatement(viaje);
			java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());
			st.setString(contador++, origen);
			st.setString(contador++, destino);
			
			rs = st.executeQuery();
			
			if (rs.next()) {
			    precio = rs.getInt("precio");
			} else {
			    throw new SQLException("No existe ese recorrido.");
			}
		} catch (SQLException e) {
			LOGGER.error("Error fetching viaje ID", e);
	        throw e;
		}
		return precio;
	}

}

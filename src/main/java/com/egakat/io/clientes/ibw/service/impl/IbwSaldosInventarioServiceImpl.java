package com.egakat.io.clientes.ibw.service.impl;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.egakat.io.clientes.ibw.domain.SaldoInventario;
import com.egakat.io.clientes.ibw.repository.SaldoInventarioRepository;
import com.egakat.io.clientes.ibw.service.api.IbwSaldosInventarioService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class IbwSaldosInventarioServiceImpl implements IbwSaldosInventarioService {

	private static final int TIPO_ARCHIVO = 110;
	private static final String NO_PROCESADO = "NO_PROCESADO";
	private static final String ESTRUCTURA_VALIDA = "ESTRUCTURA_VALIDA";

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private SaldoInventarioRepository repository;

	@Override
	@Transactional(readOnly = false)
	public void saveRecords(long archivoId, List<SaldoInventario> entities, boolean success) {
		try {
			if (success) {
				repository.saveAll(entities);
				repository.flush();
				updateArchivo(archivoId, ESTRUCTURA_VALIDA);
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		updateArchivo(archivoId, "ERROR_ESTRUCTURA");
	}

	@Override
	@Transactional(readOnly = false)
	public long insertArchivo(Path path) {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		final String sql = "INSERT INTO eIntegration.dbo.archivos " + "(id_tipo_archivo,nombre,estado,ruta) "
				+ "VALUES(:id_tipo_archivo,:nombre,:estado,:ruta)";

		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("id_tipo_archivo", TIPO_ARCHIVO);
		parameters.put("nombre", path.getFileName().toString());
		parameters.put("estado", NO_PROCESADO);
		parameters.put("ruta", path.toString());

		SqlParameterSource paramSource = new MapSqlParameterSource(parameters);
		jdbcTemplate.update(sql, paramSource, keyHolder);

		return keyHolder.getKey().longValue();
	}

	@Transactional(readOnly = false)

	public void updateArchivo(long archivoId, String estado) {
		final String sql = "UPDATE eIntegration.dbo.archivos SET estado = :estado WHERE id_archivo = :id_archivo";
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("id_archivo", archivoId);
		parameters.put("estado", estado);

		SqlParameterSource paramSource = new MapSqlParameterSource(parameters);
		jdbcTemplate.update(sql, paramSource);
	}

}

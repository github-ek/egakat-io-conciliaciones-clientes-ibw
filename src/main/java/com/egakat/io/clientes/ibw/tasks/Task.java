package com.egakat.io.clientes.ibw.tasks;

import static java.util.stream.Collectors.toList;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.egakat.io.clientes.ibw.domain.SaldoInventario;
import com.egakat.io.clientes.ibw.enums.EstadoRegistroType;
import com.egakat.io.clientes.ibw.service.api.IbwSaldosInventarioService;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@Getter
public class Task {
	private static final String CLIENTE_CODIGO = "IBW";

	private static final int INDEX_FECHA = 0;
	private static final int INDEX_PRODUCTO = 1;
	private static final int INDEX_CANTIDAD = 2;
	private static final int INDEX_VALOR = 3;
	private static final int INDEX_BODEGA = 4;

	@Value("${directories.inputs}")
	private String inputs;

	@Value("${directories.errors}")
	private String errors;

	@Value("${directories.backups}")
	private String backups;

	@Value("${cron-retries}")
	private Integer retries = 10;

	@Value("${cron-delay-between-retries}")
	private Long delayBetweenRetries = 10L * 1000L;

	@Autowired
	private IbwSaldosInventarioService service;
	

	@Scheduled(cron = "${cron}")
	public void run() {
		for (int i = 0; i < retries; i++) {
			log.debug("INTEGRACION {}: intento {} de {}", "SALDOS IBW", i + 1, retries);
			boolean success = true;
			read();

			if (success) {
				break;
			} else {
				sleep();
			}
		}
	}

	private void read() {

		try (Stream<Path> walk = Files.walk(Paths.get(inputs))) {

			List<String> files = walk.filter(Files::isRegularFile).map(x -> x.toString()).collect(toList());
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
			val ts = formatter.format(LocalDateTime.now());
			val directory = Paths.get(backups, ts.substring(0, 6), ts.substring(0, 8));

			if (createDirectoryIfNotExists(directory)) {
				for (String file : files) {
					val source = Paths.get(file);
					val target = Paths.get(directory.toString(), ts + "-" + source.getFileName().toString());
					val error = Paths.get(errors, ts + "-" + source.getFileName().toString());

					List<List<String>> data = new ArrayList<>();
					try {
						data.addAll(readFile(file));
						Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
					} catch (Exception e) {
						log.error("", e);
						Files.move(source, error, StandardCopyOption.REPLACE_EXISTING);
					}
					loadData(target, data);
				}
			}
		} catch (IOException e) {
			log.error("", e);
		}
	}

	private boolean createDirectoryIfNotExists(final Path directory) {
		boolean result = true;
		if (Files.notExists(directory)) {
			try {
				Files.createDirectories(directory);
			} catch (IOException e) {
				e.printStackTrace();
				result = false;
			}
		}
		return result;
	}

	private List<List<String>> readFile(String file) throws IOException {
		val result = new ArrayList<List<String>>();
		val parser = new CSVParserBuilder().withSeparator('\t').build();

		try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(file)).withCSVParser(parser).build()) {
			String[] nextRecord;

			while ((nextRecord = csvReader.readNext()) != null) {
				val record = new ArrayList<String>();
				for (String cell : nextRecord) {
					record.add(cell.toUpperCase());
				}
				result.add(record);
			}
		} catch (Exception e) {
			result.clear();
			e.printStackTrace();
		}
		return result;
	}

	private void loadData(Path path, List<List<String>> data) {
		long archivoId =service.insertArchivo(path);

		val entities = new ArrayList<SaldoInventario>();
		int index = 0;
		boolean success = true;
		for (List<String> record : data) {
			try {
				val entity = asEntity(archivoId, index++, record);
				entities.add(entity);
			} catch (Exception e) {
				e.printStackTrace();
				success = false;
			}
		}

		service.saveRecords(archivoId, entities, success);
	}


	private SaldoInventario asEntity(long archivoId, int index, List<String> record) {
		LocalDateTime fechaCorte = getLocalDateTime(record.get(INDEX_FECHA));
		Integer unidadesPorEmpaque = 1;
		BigDecimal cantidad = BigDecimal.valueOf(Long.parseLong(record.get(INDEX_CANTIDAD)));
		Integer valorUnitario = Integer.parseInt(record.get(INDEX_VALOR));

		val entity = new SaldoInventario();

		entity.setIdArchivo(archivoId);
		entity.setEstado(EstadoRegistroType.ESTRUCTURA_VALIDA.toString());
		entity.setNumeroLinea(index);
		entity.setClienteCodigo(CLIENTE_CODIGO);
		entity.setFecha(fechaCorte.toLocalDate());
		entity.setFechaCorte(fechaCorte);
		entity.setProductoCodigoAlterno(record.get(INDEX_PRODUCTO));
		entity.setBodegaCodigoAlterno(record.get(INDEX_BODEGA));
		entity.setEstadoConciliacionCodigoAlterno(record.get(INDEX_BODEGA));
		entity.setUnidadMedidaCodigoAlterno("");
		entity.setUnidadesPorEmpaque(unidadesPorEmpaque);
		entity.setCantidad(cantidad);
		entity.setValorUnitario(valorUnitario.intValue());

		return entity;
	}

	private LocalDateTime getLocalDateTime(String date) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy H:mm:ss");
		return LocalDateTime.parse(date, formatter);
	}

	private void sleep() {
		try {
			Thread.sleep(delayBetweenRetries * 1000);
		} catch (InterruptedException e) {
			;
		}
	}
}

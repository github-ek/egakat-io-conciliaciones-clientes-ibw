package com.egakat.io.clientes.ibw.service.api;

import java.nio.file.Path;
import java.util.List;

import com.egakat.io.clientes.ibw.domain.SaldoInventario;

public interface IbwSaldosInventarioService {

	long insertArchivo(Path path);

	void saveRecords(long archivoId, List<SaldoInventario> entities, boolean success);
}

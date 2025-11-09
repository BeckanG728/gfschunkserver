package com.tpdteam3.chunkserver.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Service
public class ChunkStorageService {

    @Value("${chunkserver.storage.path:./storage}")
    private String storagePath;

    @Value("${server.port:9001}")
    private int serverPort;

    @PostConstruct
    public void init() throws IOException {
        // Crear directorio de almacenamiento si no existe
        Path path = Paths.get(storagePath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        System.out.println("✅ Chunkserver iniciado en puerto " + serverPort);
        System.out.println("✅ Almacenamiento en: " + path.toAbsolutePath());
        System.out.println("✅ Modo: PERSISTENCIA EN DISCO");
    }

    /**
     * Almacena un fragmento EN DISCO
     */
    public void writeChunk(String imagenId, int chunkIndex, String base64Data) {
        try {
            String filename = generateFilename(imagenId, chunkIndex);
            Path filePath = Paths.get(storagePath, filename);

            byte[] data = Base64.getDecoder().decode(base64Data);
            Files.write(filePath, data);

            System.out.println("✅ Fragmento guardado en disco: " + filename + " (" + data.length + " bytes)");
        } catch (IOException e) {
            throw new RuntimeException("Error escribiendo fragmento a disco: " + e.getMessage(), e);
        }
    }

    /**
     * Lee un fragmento DESDE DISCO
     */
    public byte[] readChunk(String imagenId, int chunkIndex) {
        try {
            String filename = generateFilename(imagenId, chunkIndex);
            Path filePath = Paths.get(storagePath, filename);

            if (!Files.exists(filePath)) {
                throw new RuntimeException("Fragmento no encontrado: " + filename);
            }

            byte[] data = Files.readAllBytes(filePath);
            System.out.println("✅ Fragmento leído desde disco: " + filename + " (" + data.length + " bytes)");
            return data;
        } catch (IOException e) {
            throw new RuntimeException("Error leyendo fragmento desde disco: " + e.getMessage(), e);
        }
    }

    /**
     * Elimina un fragmento DEL DISCO
     */
    public void deleteChunk(String imagenId, int chunkIndex) {
        try {
            String filename = generateFilename(imagenId, chunkIndex);
            Path filePath = Paths.get(storagePath, filename);

            if (Files.exists(filePath)) {
                Files.delete(filePath);
                System.out.println("🗑️ Fragmento eliminado del disco: " + filename);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error eliminando fragmento del disco: " + e.getMessage(), e);
        }
    }

    /**
     * Elimina todos los fragmentos de una imagen DEL DISCO
     */
    public void deleteAllChunks(String imagenId) {
        try {
            Path dir = Paths.get(storagePath);
            String prefix = imagenId + "_chunk_";

            try (Stream<Path> files = Files.list(dir)) {
                long deletedCount = files
                        .filter(path -> path.getFileName().toString().startsWith(prefix))
                        .peek(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                System.err.println("Error eliminando: " + path);
                            }
                        })
                        .count();

                System.out.println("🗑️ Eliminados " + deletedCount + " fragmentos para imagen: " + imagenId);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error eliminando fragmentos: " + e.getMessage(), e);
        }
    }

    /**
     * Obtiene estadísticas del servidor (DESDE DISCO)
     */
    public Map<String, Object> getStats() {
        try {
            Map<String, Object> stats = new HashMap<>();
            Path dir = Paths.get(storagePath);

            if (!Files.exists(dir)) {
                stats.put("totalChunks", 0);
                stats.put("totalStorageUsed", 0L);
                stats.put("storageUsedMB", 0.0);
                return stats;
            }

            try (Stream<Path> files = Files.list(dir)) {
                long[] totalSize = {0};
                long count = files
                        .filter(Files::isRegularFile)
                        .peek(path -> {
                            try {
                                totalSize[0] += Files.size(path);
                            } catch (IOException e) {
                                // Ignorar
                            }
                        })
                        .count();

                stats.put("totalChunks", count);
                stats.put("totalStorageUsed", totalSize[0]);
                stats.put("storageUsedMB", totalSize[0] / (1024.0 * 1024.0));
                stats.put("storagePath", dir.toAbsolutePath().toString());
            }

            return stats;
        } catch (IOException e) {
            throw new RuntimeException("Error obteniendo estadísticas: " + e.getMessage(), e);
        }
    }

    /**
     * Verifica si un fragmento existe EN DISCO
     */
    public boolean chunkExists(String imagenId, int chunkIndex) {
        String filename = generateFilename(imagenId, chunkIndex);
        Path filePath = Paths.get(storagePath, filename);
        return Files.exists(filePath);
    }

    /**
     * Genera nombre de archivo único para un fragmento
     */
    private String generateFilename(String imagenId, int chunkIndex) {
        return imagenId + "_chunk_" + chunkIndex + ".bin";
    }
}
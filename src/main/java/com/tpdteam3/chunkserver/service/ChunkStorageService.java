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
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ChunkStorageService {

    @Value("${chunkserver.storage.path:./storage}")
    private String storagePath;

    @Value("${server.port:9001}")
    private int serverPort;

    // Almacena fragmentos en memoria (para simplificar)
    // En producción, se escribirían a disco
    private final Map<String, byte[]> chunkStore = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() throws IOException {
        // Crear directorio de almacenamiento si no existe
        Path path = Paths.get(storagePath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        System.out.println("Chunkserver iniciado en puerto " + serverPort);
        System.out.println("Almacenamiento en: " + path.toAbsolutePath());
    }

    /**
     * Almacena un fragmento
     */
    public void writeChunk(String imagenId, int chunkIndex, String base64Data) {
        String key = generateKey(imagenId, chunkIndex);
        byte[] data = Base64.getDecoder().decode(base64Data);
        chunkStore.put(key, data);

        System.out.println("Fragmento almacenado: " + key + " (" + data.length + " bytes)");
    }

    /**
     * Lee un fragmento
     */
    public byte[] readChunk(String imagenId, int chunkIndex) {
        String key = generateKey(imagenId, chunkIndex);
        byte[] data = chunkStore.get(key);

        if (data == null) {
            throw new RuntimeException("Fragmento no encontrado: " + key);
        }

        System.out.println("Fragmento leído: " + key + " (" + data.length + " bytes)");
        return data;
    }

    /**
     * Elimina un fragmento
     */
    public void deleteChunk(String imagenId, int chunkIndex) {
        String key = generateKey(imagenId, chunkIndex);
        chunkStore.remove(key);
        System.out.println("Fragmento eliminado: " + key);
    }

    /**
     * Elimina todos los fragmentos de una imagen
     */
    public void deleteAllChunks(String imagenId) {
        chunkStore.keySet().removeIf(key -> key.startsWith(imagenId + ":"));
        System.out.println("Todos los fragmentos eliminados para: " + imagenId);
    }

    /**
     * Obtiene estadísticas del servidor
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalChunks", chunkStore.size());

        long totalSize = 0;
        for (byte[] data : chunkStore.values()) {
            totalSize += data.length;
        }
        stats.put("totalStorageUsed", totalSize);
        stats.put("storageUsedMB", totalSize / (1024.0 * 1024.0));

        return stats;
    }

    /**
     * Verifica si un fragmento existe
     */
    public boolean chunkExists(String imagenId, int chunkIndex) {
        String key = generateKey(imagenId, chunkIndex);
        return chunkStore.containsKey(key);
    }

    /**
     * Genera clave única para un fragmento
     */
    private String generateKey(String imagenId, int chunkIndex) {
        return imagenId + ":" + chunkIndex;
    }
}

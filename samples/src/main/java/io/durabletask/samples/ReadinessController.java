package io.durabletask.samples;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/ready")
public class ReadinessController {
    @GetMapping
    public ResponseEntity<String> readiness() {
        return ResponseEntity.ok("Application is ready");
    }
}

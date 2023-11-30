package com.xiaoxin.controller;

import com.xiaoxin.handler.NonStaticResourceHttpRequestHandler;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.File;
import java.nio.charset.StandardCharsets;

@Controller
public class VideoController {
    @Autowired
    private NonStaticResourceHttpRequestHandler nonStaticResourceHttpRequestHandler;

    @GetMapping("/video")
    public void getVideo(HttpServletRequest request, HttpServletResponse response) {
        String path = "D:\\hhh.mp4";
        try {
            File file = new File(path);
            if (file.exists()) {
                request.setAttribute(NonStaticResourceHttpRequestHandler.ATTR_FILE, path);
                nonStaticResourceHttpRequestHandler.handleRequest(request, response);
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.setCharacterEncoding(StandardCharsets.UTF_8.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @RequestMapping("/vv")
    public String video() {
        return "Video";
    }
}

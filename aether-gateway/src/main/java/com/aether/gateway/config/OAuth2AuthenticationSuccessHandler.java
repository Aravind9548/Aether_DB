package com.aether.gateway.config;

import com.aether.gateway.model.User;
import com.aether.gateway.repository.UserRepository;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class OAuth2AuthenticationSuccessHandler extends SimpleUrlAuthenticationSuccessHandler {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    private JwtUtils jwtUtils;

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
                                        Authentication authentication) throws IOException, ServletException {
        
        OAuth2User oAuth2User = (OAuth2User) authentication.getPrincipal();
        String email = oAuth2User.getAttribute("email");
        
        if (email == null) {
            response.sendRedirect("/?error=email_not_provided");
            return;
        }

        if (userRepository.findByUsername(email).isEmpty()) {
            User newUser = new User(email, passwordEncoder.encode(UUID.randomUUID().toString()));
            userRepository.save(newUser);
        }

        String token = jwtUtils.generateToken(email);
        String targetUrl = "/?token=" + token + "&tenant=" + email;
        getRedirectStrategy().sendRedirect(request, response, targetUrl);
    }
}

package com.ycu.tang.msbplatform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication()
@ComponentScan(basePackages = {"com.ycu.tang.msbplatform"})
@EnableScheduling
public class Application {

	/**
	 * コンストラクタ.<br>
	 * インスタンス生成されないようprivateとする.
	 */
	public Application() {
		// do nothing
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}

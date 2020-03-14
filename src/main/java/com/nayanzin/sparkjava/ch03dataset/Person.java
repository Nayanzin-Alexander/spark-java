package com.nayanzin.sparkjava.ch03dataset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person implements Serializable {
    private String name;
    private Integer age;
    private String state;
    @EqualsAndHashCode.Exclude
    private BigDecimal salary;

    @SuppressWarnings("unused")
    @EqualsAndHashCode.Include
    private Object salaryForEqHC() {
        return salary.stripTrailingZeros();
    }
}

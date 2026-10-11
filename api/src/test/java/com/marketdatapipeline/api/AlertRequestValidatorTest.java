package com.marketdatapipeline.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class AlertRequestValidatorTest {

  @Test
  void aboveIsAValidDirection() {
    assertThatCode(() -> AlertRequestValidator.requireDirection("above")).doesNotThrowAnyException();
  }

  @Test
  void belowIsAValidDirection() {
    assertThatCode(() -> AlertRequestValidator.requireDirection("below")).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"Above", "ABOVE", "Below", "sideways", "equal"})
  void unknownOrWrongCaseDirectionIsRejected(String direction) {
    assertThatThrownBy(() -> AlertRequestValidator.requireDirection(direction))
        .isInstanceOf(InvalidAlertRequestException.class);
  }

  @ParameterizedTest
  @NullAndEmptySource
  void nullOrEmptyDirectionIsRejected(String direction) {
    assertThatThrownBy(() -> AlertRequestValidator.requireDirection(direction))
        .isInstanceOf(InvalidAlertRequestException.class);
  }

  @Test
  void parsesAnOrdinaryPositiveThreshold() {
    assertThat(AlertRequestValidator.parseThreshold("64012.55")).isEqualTo(64012.55);
  }

  @ParameterizedTest
  @ValueSource(strings = {"not-a-number", "1e", "NaN", "Infinity"})
  void nonNumericOrNonFiniteThresholdIsRejected(String threshold) {
    assertThatThrownBy(() -> AlertRequestValidator.parseThreshold(threshold))
        .isInstanceOf(InvalidAlertRequestException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"-5", "-0.01", "0", "0.0"})
  void negativeOrZeroThresholdIsRejected(String threshold) {
    assertThatThrownBy(() -> AlertRequestValidator.parseThreshold(threshold))
        .isInstanceOf(InvalidAlertRequestException.class);
  }

  @ParameterizedTest
  @NullAndEmptySource
  void nullOrEmptyThresholdIsRejected(String threshold) {
    assertThatThrownBy(() -> AlertRequestValidator.parseThreshold(threshold))
        .isInstanceOf(InvalidAlertRequestException.class);
  }
}

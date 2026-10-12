package com.marketdatapipeline.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class CrossingDetectorTest {

  private static final double THRESHOLD = 100.0;

  @Test
  void firstTickEverHasNoPreviousPriceSoNeverCrosses() {
    assertThat(CrossingDetector.crossed(null, 150.0, "above", THRESHOLD)).isFalse();
    assertThat(CrossingDetector.crossed(null, 50.0, "below", THRESHOLD)).isFalse();
  }

  @Test
  void aboveFiresOnlyOnTheUpwardTransitionThroughTheThreshold() {
    assertThat(CrossingDetector.crossed(99.0, 100.0, "above", THRESHOLD)).isFalse();
    assertThat(CrossingDetector.crossed(100.0, 100.0, "above", THRESHOLD)).isFalse();
    assertThat(CrossingDetector.crossed(100.0, 101.0, "above", THRESHOLD)).isTrue();
    assertThat(CrossingDetector.crossed(101.0, 102.0, "above", THRESHOLD)).isFalse();
  }

  @Test
  void belowFiresOnlyOnTheDownwardTransitionThroughTheThreshold() {
    assertThat(CrossingDetector.crossed(101.0, 100.0, "below", THRESHOLD)).isFalse();
    assertThat(CrossingDetector.crossed(100.0, 100.0, "below", THRESHOLD)).isFalse();
    assertThat(CrossingDetector.crossed(100.0, 99.0, "below", THRESHOLD)).isTrue();
    assertThat(CrossingDetector.crossed(99.0, 98.0, "below", THRESHOLD)).isFalse();
  }

  // Requested sequence: 100, 100, 101 (threshold 100, "above"). Resting exactly at the
  // threshold for a tick must not itself read as "crossed," and must not suppress the
  // real crossing once it actually happens on the next tick.
  @Test
  void restingAtTheThresholdThenCrossingFiresExactlyOnTheRealTransition() {
    Double previous = null;
    boolean[] fired = new boolean[3];
    double[] prices = {100.0, 100.0, 101.0};
    for (int i = 0; i < prices.length; i++) {
      fired[i] = CrossingDetector.crossed(previous, prices[i], "above", THRESHOLD);
      previous = prices[i];
    }
    assertThat(fired).containsExactly(false, false, true);
  }

  // Requested sequence: 101, 100, 100, 101 (threshold 100, "above"). The dip down to and
  // resting at the threshold must not register as a crossing on the way down (wrong
  // direction) or while flat, only on the final upward transition.
  @Test
  void dipToThresholdThenBackAboveFiresOnlyOnTheFinalTransition() {
    Double previous = null;
    boolean[] fired = new boolean[4];
    double[] prices = {101.0, 100.0, 100.0, 101.0};
    for (int i = 0; i < prices.length; i++) {
      fired[i] = CrossingDetector.crossed(previous, prices[i], "above", THRESHOLD);
      previous = prices[i];
    }
    assertThat(fired).containsExactly(false, false, false, true);
  }

  // The point this test exists to prove, for DECISIONS.md: the detector is memoryless and
  // will report "crossed" again for a second, later excursion past the same threshold —
  // it has no idea an alert "already fired" once. Two genuinely separate upward crossings
  // (100->101, dip back to 100, then 100->101 again) both read true at this level. What
  // actually makes a real alert fire at most once is AlertStore's armed-only filter
  // excluding it from ever being evaluated again after the first fire — not this class.
  @Test
  void detectorAloneReportsTrueForEachSeparateCrossingNotJustTheFirstEver() {
    assertThat(CrossingDetector.crossed(100.0, 101.0, "above", THRESHOLD)).isTrue();
    assertThat(CrossingDetector.crossed(101.0, 100.0, "above", THRESHOLD)).isFalse();
    assertThat(CrossingDetector.crossed(100.0, 101.0, "above", THRESHOLD)).isTrue();
  }

  @Test
  void unknownDirectionIsRejected() {
    assertThatThrownBy(() -> CrossingDetector.crossed(100.0, 101.0, "sideways", THRESHOLD))
        .isInstanceOf(IllegalArgumentException.class);
  }
}

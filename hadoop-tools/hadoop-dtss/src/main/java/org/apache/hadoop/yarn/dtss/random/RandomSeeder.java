package org.apache.hadoop.yarn.dtss.random;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.dtss.config.parameters.RandomSeed;

import javax.annotation.Nullable;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generates new {@link Random} objects with random seeds.
 */
@InterfaceAudience.Private
@Singleton
public final class RandomSeeder {
  private static final Logger LOG = Logger.getLogger(RandomSeeder.class.getName());

  private final Random random;

  @Inject
  private RandomSeeder(@Nullable @RandomSeed final Long seed) {
    if (seed != null) {
      LOG.log(Level.INFO, String.format("Initializing RandomSeeder object with seed %d.", seed));
      this.random = new Random(seed);
    } else {
      LOG.log(Level.INFO, "Initializing RandomSeeder object without a seed.");
      this.random = new Random();
    }
  }

  public RandomGenerator newRandomGenerator() {
    return new RandomGenerator(random.nextLong());
  }
}

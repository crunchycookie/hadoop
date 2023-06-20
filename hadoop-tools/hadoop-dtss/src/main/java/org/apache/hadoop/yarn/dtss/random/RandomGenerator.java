package org.apache.hadoop.yarn.dtss.random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Random;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A wrapper around the {@link Random} class.
 */
@InterfaceAudience.Private
public final class RandomGenerator {
  private static final Logger LOG = Logger.getLogger(RandomGenerator.class.getName());

  private final Random random;

  RandomGenerator(final long seed) {
    random = new Random(seed);
    LOG.log(Level.INFO, String.format("Initializing RandomGenerator object with seed %d.", seed));
  }

  public int randomInt(final int exclusiveMax) {
    return random.nextInt(exclusiveMax);
  }

  public int randomInt(final int inclusiveMin, final int exclusiveMax) {
    final int diff = exclusiveMax - inclusiveMin;
    return inclusiveMin + random.nextInt(diff);
  }

  public double randomDouble(final double inclusiveMin, final double inclusiveMax) {
    return inclusiveMin + (inclusiveMax - inclusiveMin) * random.nextDouble();
  }

  public UUID randomUUID() {
    final byte[] randomBytes = RandomStringUtils
        .random(25, 0, 0, false, false, null, random)
        .getBytes();

    return UUID.nameUUIDFromBytes(randomBytes);
  }
}


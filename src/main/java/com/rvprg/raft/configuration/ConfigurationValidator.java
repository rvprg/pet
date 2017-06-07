package com.rvprg.raft.configuration;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

public class ConfigurationValidator implements ConstraintValidator<ValidConfiguration, Configuration> {

    @Override
    public void initialize(ValidConfiguration constraintAnnotation) {
    }

    @Override
    public boolean isValid(Configuration configuration, ConstraintValidatorContext context) {
        HibernateConstraintValidatorContext hibernateContext = context.unwrap(
                HibernateConstraintValidatorContext.class);

        boolean isValid = true;

        if (configuration.getElectionMinTimeout() > configuration.getElectionMaxTimeout()) {
            hibernateContext
                    .buildConstraintViolationWithTemplate("electionMinTimeout must be smaller than electionMaxTimeout")
                    .addConstraintViolation();
            isValid = false;
        }

        if (!configuration.getSnapshotFolderPath().exists() ||
                !configuration.getSnapshotFolderPath().isDirectory() ||
                !configuration.getSnapshotFolderPath().canWrite()) {
            hibernateContext
                    .buildConstraintViolationWithTemplate("snapshotFolderPath should point to existing folder and be writable")
                    .addConstraintViolation();
            isValid = false;
        }

        if (!isValid) {
            hibernateContext.disableDefaultConstraintViolation();
        }

        return isValid;
    }

}

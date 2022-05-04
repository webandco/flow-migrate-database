<?php

namespace Webandco\MigrateDatabase\Persistence\Doctrine;

use Neos\Flow\Annotations as Flow;
use Neos\Flow\Configuration\ConfigurationManager;
use Neos\Flow\Configuration\Exception\InvalidConfigurationException;
use Neos\Flow\Core\Bootstrap;

/**
 * @Flow\Scope("singleton")
 */
class EntityManagerFactory extends \Neos\Flow\Persistence\Doctrine\EntityManagerFactory
{
    /**
     * Injects Flow settings or custom Webandco.MigrateDatabase.connections.[name] settings if a corresponding environment variable exists
     *
     * @param array $settings
     * @return void
     * @throws InvalidConfigurationException
     */
    public function injectSettings(array $settings)
    {
        $settings = $this->preprocessInjectSettings($settings, getenv('WEBANDCO_MIGRATEDATABASE'));
        parent::injectSettings($settings);
    }

    /**
     * Determine if a configuration with the given name is available
     *
     * @param array $settings
     * @param string|null $name
     * @return array|mixed
     * @throws \Neos\Flow\Configuration\Exception\InvalidConfigurationTypeException
     */
    protected function preprocessInjectSettings(array $settings, string $name = null)
    {
        if ($name) {
            if (isset($settings['connections'][$name])) {
                $settings = $settings['connections'][$name];
            } else {
                throw new \Exception('Missing connection configuration Webandco.MigrateDatabase.connections.' . $name,
                    1651573774775);
            }
        } else {
            // using staticObjectManager because EntityManager is instantiated very early
            $configurationManager = Bootstrap::$staticObjectManager->get(ConfigurationManager::class);
            $settings = $configurationManager->getConfiguration(ConfigurationManager::CONFIGURATION_TYPE_SETTINGS,
                'Neos.Flow');
        }

        return $settings;
    }

    /**
     * @param string $name Name of the configuration from Webandco.MigrateDatabase.connections.[name] to load for the EntityManager
     * @return \Doctrine\ORM\EntityManager
     * @throws InvalidConfigurationException
     * @throws \Neos\Flow\Configuration\Exception\InvalidConfigurationTypeException
     */
    public function createEntityManagerByName(string $name)
    {
        $tmpSettings = $this->settings;

        $configurationManager = $this->objectManager->get(ConfigurationManager::class);
        $settings = $configurationManager->getConfiguration(ConfigurationManager::CONFIGURATION_TYPE_SETTINGS,
            'Webandco.MigrateDatabase');

        $settings = $this->preprocessInjectSettings($settings, $name);
        parent::injectSettings($settings);

        $entityManager = parent::create();

        $this->settings = $tmpSettings;

        return $entityManager;
    }
}

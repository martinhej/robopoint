<?php

namespace robopoint\DependencyInjection;

use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\Extension;

class RobopointExtension extends Extension {

    public function load(array $configs, ContainerBuilder $container)
    {
        $configuration = new Configuration();
        $processor = new Processor();
        $config = $processor->processConfiguration($configuration, $configs);
        $container->setParameter('robopoint', $config);
    }

    public function getAlias()
    {
        return 'robopoint';
    }

}

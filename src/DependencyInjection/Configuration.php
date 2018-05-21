<?php

namespace robopoint\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

/**
 * Class Configuration.
 *
 * @package robopoint\DependencyInjection
 */
class Configuration implements ConfigurationInterface {

    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('robopoint');

        $rootNode
            ->children()
                ->scalarNode('stream_name')->isRequired()->end()
                ->scalarNode('message_schema_dir')->isRequired()->end()
                ->scalarNode('message_schema_version')->defaultValue('v_0_1')->end()
            ->end();

        $this->addKinesisConfiguration($rootNode);
        $this->addDynamoDBConfiguration($rootNode);

        return $treeBuilder;
    }

    protected function addKinesisConfiguration(ArrayNodeDefinition $rootNode) {
        $rootNode->children()
            ->arrayNode('kinesis')
                ->children()
                    ->scalarNode('api_version')->defaultValue('2013-12-02')->end()
                    ->scalarNode('region')->isRequired()->end()
                    ->arrayNode('consumer')
                        ->children()
                            ->scalarNode('recovery_file')->isRequired()->end()
                            ->scalarNode('key')->isRequired()->end()
                            ->scalarNode('secret')->isRequired()->end()
                        ->end()
                    ->end()
                    ->arrayNode('producer')
                        ->children()
                            ->scalarNode('key')->isRequired()->end()
                            ->scalarNode('secret')->isRequired()->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ->end();
    }

    protected function addDynamoDBConfiguration(ArrayNodeDefinition $rootNode) {
        $rootNode->children()
            ->arrayNode('dynamodb')
                ->children()
                    ->scalarNode('api_version')->defaultValue('2012-08-10')->end()
                    ->scalarNode('region')->isRequired()->end()
                ->end()
            ->end()
        ->end();
    }
}
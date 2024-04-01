<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class CreateReplicationsConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->scalarNode('sourceKbcUrl')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('sourceHost')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('sourceUsername')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('#sourcePassword')->isRequired()->cannotBeEmpty()->end()
                ->integerNode('projectIdFrom')->isRequired()->end()
                ->integerNode('projectIdTo')->isRequired()->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
